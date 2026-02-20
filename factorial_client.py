import calendar
import sys
import time
from collections import defaultdict

import requests


class FactorialClient:
    """Client for Factorial HR API (v2025-01-01)."""

    BASE_URL = "https://api.factorialhr.com"
    API_VERSION = "2025-01-01"

    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "x-api-key": config.factorial_api_key,
        })

    # ── HTTP helpers ─────────────────────────────────────────────

    def _request(self, method, url, max_retries=3, **kwargs):
        for attempt in range(max_retries):
            try:
                resp = self.session.request(method, url, **kwargs)

                if resp.status_code == 200:
                    return resp

                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 10))
                    print(f"  Factorial rate limit. Esperando {wait}s...")
                    time.sleep(wait)
                    continue

                if resp.status_code == 401:
                    print("Error: Autenticación Factorial fallida. Revisa FACTORIAL_API_KEY.")
                    sys.exit(1)

                if resp.status_code >= 500:
                    wait = 2 ** attempt
                    print(f"  Factorial error servidor ({resp.status_code}). Reintento en {wait}s...")
                    time.sleep(wait)
                    continue

                print(f"Factorial error {resp.status_code}: {resp.text[:300]}")
                raise RuntimeError(f"Factorial HTTP {resp.status_code}")

            except requests.exceptions.ConnectionError:
                wait = 2 ** attempt
                print(f"  Factorial error de conexión. Reintento en {wait}s...")
                time.sleep(wait)

        print(f"Factorial: fallo tras {max_retries} reintentos: {url}")
        raise RuntimeError(f"Factorial failed after {max_retries} retries: {url}")

    def _url(self, resource):
        """Build versioned API URL."""
        return f"{self.BASE_URL}/api/{self.API_VERSION}/resources/{resource}"

    # ── Pagination ───────────────────────────────────────────────

    def _paginate(self, url, params=None, limit=100):
        """Page-based pagination. Response: {"data": [...], "meta": {...}}."""
        params = dict(params or {})
        params["limit"] = limit
        page = 1
        results = []

        while True:
            params["page"] = page
            data = self._request("GET", url, params=params).json()
            items = data.get("data", [])
            if not isinstance(items, list):
                break
            results.extend(items)
            meta = data.get("meta", {})
            if not meta.get("has_next_page", False) or len(items) < limit:
                break
            page += 1

        return results

    # ── Employees ────────────────────────────────────────────────

    def get_employees(self):
        """GET employees — all employees (active + terminated)."""
        url = self._url("employees/employees")
        employees = self._paginate(url)
        print(f"  {len(employees)} empleados en Factorial")
        return employees

    def get_employees_map(self):
        """Returns {email_lower: {id, full_name, email, start_date, terminated_on}}."""
        raw = self.get_employees()
        result = {}
        for emp in raw:
            email = (emp.get("email") or "").strip().lower()
            if not email:
                continue
            full_name = emp.get("full_name") or f'{emp.get("first_name", "")} {emp.get("last_name", "")}'.strip()
            result[email] = {
                "id": emp.get("id"),
                "full_name": full_name,
                "email": email,
                "start_date": emp.get("start_date"),
                "terminated_on": emp.get("terminated_on"),
            }
        return result

    # ── Attendance / Clock-in hours ──────────────────────────────

    def get_attendance(self, year, month):
        """GET attendance/shifts — clock-in/out records for a month."""
        url = self._url("attendance/shifts")
        params = {"year": year, "month": month}
        return self._paginate(url, params=params)

    def get_attendance_range(self, date_from, date_to):
        """Fetch attendance per month in range YYYY-MM to YYYY-MM.

        Returns: (monthly, daily)
          monthly: {employee_id -> {month_key -> total_hours}}
          daily:   {employee_id -> {date_str -> total_hours}}
        """
        monthly = defaultdict(lambda: defaultdict(float))
        daily = defaultdict(lambda: defaultdict(float))

        y, m = int(date_from[:4]), int(date_from[5:7])
        end_y, end_m = int(date_to[:4]), int(date_to[5:7])

        while (y, m) <= (end_y, end_m):
            month_key = f"{y}-{m:02d}"
            print(f"  Factorial fichajes {month_key}...")
            records = self.get_attendance(y, m)
            # Debug: show first record structure
            if records and month_key == f"{y}-{m:02d}" and not hasattr(self, '_debug_shown'):
                sample = records[0]
                print(f"    [DEBUG] Ejemplo registro: {dict((k, sample.get(k)) for k in ('employee_id', 'date', 'clock_in', 'clock_out', 'minutes', 'observations') if k in sample)}")
                print(f"    [DEBUG] Claves disponibles: {list(sample.keys())}")
                self._debug_shown = True
            skipped = 0
            month_hours = 0
            for rec in records:
                emp_id = rec.get("employee_id")
                clock_in = rec.get("clock_in") or ""
                clock_out = rec.get("clock_out") or ""
                if not emp_id or not clock_in or not clock_out:
                    skipped += 1
                    continue
                try:
                    hours = 0
                    day_str = rec.get("date") or ""
                    minutes = rec.get("minutes")
                    if minutes is not None and minutes > 0:
                        hours = minutes / 60
                    elif "T" in clock_in:
                        from datetime import datetime as dt
                        t_in = dt.fromisoformat(clock_in.replace("Z", "+00:00"))
                        t_out = dt.fromisoformat(clock_out.replace("Z", "+00:00"))
                        hours = (t_out - t_in).total_seconds() / 3600
                        if not day_str:
                            day_str = t_in.strftime("%Y-%m-%d")
                    else:
                        h_in, m_in = map(int, clock_in.split(":"))
                        h_out, m_out = map(int, clock_out.split(":"))
                        hours = (h_out * 60 + m_out - h_in * 60 - m_in) / 60
                    if hours > 0:
                        monthly[emp_id][month_key] += hours
                        if day_str:
                            daily[emp_id][day_str] += hours
                        month_hours += hours
                except (ValueError, TypeError):
                    skipped += 1
            print(f"    {len(records)} registros, {skipped} saltados, {month_hours:.1f}h total")
            m += 1
            if m > 12:
                m = 1
                y += 1

        return monthly, daily

    # ── Leaves / Absences ────────────────────────────────────────

    def get_leaves(self):
        """GET timeoff/leaves — all leave records."""
        url = self._url("timeoff/leaves")
        return self._paginate(url)

    def get_leaves_in_range(self, date_from, date_to):
        """Fetch leaves overlapping the date range.

        Returns: {employee_id -> [{start_date, end_date, leave_type, status}]}
        """
        all_leaves = self.get_leaves()
        to_y, to_m = int(date_to[:4]), int(date_to[5:7])
        last_day = calendar.monthrange(to_y, to_m)[1]
        range_start = f"{date_from}-01"
        range_end = f"{date_to}-{last_day:02d}"

        result = defaultdict(list)
        for leave in all_leaves:
            # New API uses 'approved' boolean; old used 'status' string
            approved = leave.get("approved")
            if approved is not None:
                if not approved:
                    continue
                status = "approved"
            else:
                status = (leave.get("status") or "").lower()
                if status not in ("approved", "pending"):
                    continue

            start = leave.get("start_on") or leave.get("start_date") or ""
            end = leave.get("finish_on") or leave.get("end_date") or ""
            if not start:
                continue
            if end >= range_start and start <= range_end:
                emp_id = leave.get("employee_id")
                if emp_id:
                    result[emp_id].append({
                        "start_date": start,
                        "end_date": end,
                        "leave_type": leave.get("leave_type_name")
                                      or leave.get("description")
                                      or "Ausencia",
                        "status": status,
                    })

        print(f"  {sum(len(v) for v in result.values())} ausencias en rango")
        return result
