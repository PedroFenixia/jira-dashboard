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

    # ── Break Configurations ─────────────────────────────────────

    def get_break_configurations(self):
        """GET attendance/break-configurations — paid/unpaid break configs.

        Returns: {config_id: {name, paid}} indexed by both id and
        time_settings_break_configuration_id for flexible lookup.
        """
        url = self._url("attendance/break-configurations")
        configs = self._paginate(url)
        result = {}
        for cfg in configs:
            paid = cfg.get("paid", False)
            name = cfg.get("name", "")
            for key_field in ("id", "time_settings_break_configuration_id"):
                key = cfg.get(key_field)
                if key:
                    result[key] = {"name": name, "paid": paid}
        paid_count = sum(1 for c in configs if c.get("paid"))
        print(f"    {len(configs)} config. pausas ({paid_count} pagadas)")
        return result

    # ── Attendance / Clock-in hours ──────────────────────────────

    def get_attendance(self, year, month):
        """GET attendance/shifts — clock-in/out records for a month."""
        url = self._url("attendance/shifts")
        params = {"year": year, "month": month}
        return self._paginate(url, params=params)

    def get_attendance_range(self, date_from, date_to):
        """Fetch attendance per month in range YYYY-MM to YYYY-MM.

        The API returns ALL records regardless of year/month params,
        so we fetch once and filter by date field.

        Includes paid break time: shift records with workable=false
        that have a paid break configuration get their minutes added.

        Returns: (monthly, daily)
          monthly: {employee_id -> {month_key -> total_hours}}
          daily:   {employee_id -> {date_str -> total_hours}}
        """
        monthly = defaultdict(lambda: defaultdict(float))
        daily = defaultdict(lambda: defaultdict(float))

        # Build set of valid months in range
        valid_months = set()
        y, m = int(date_from[:4]), int(date_from[5:7])
        end_y, end_m = int(date_to[:4]), int(date_to[5:7])
        while (y, m) <= (end_y, end_m):
            valid_months.add(f"{y}-{m:02d}")
            m += 1
            if m > 12:
                m = 1
                y += 1

        # Fetch break configurations to identify paid breaks
        break_configs = self.get_break_configurations()

        # Fetch all records (API ignores year/month filter)
        print(f"  Factorial fichajes ({len(valid_months)} meses: {date_from} a {date_to})...")
        records = self.get_attendance(int(date_from[:4]), int(date_from[5:7]))
        print(f"    {len(records)} registros totales recibidos")

        skipped = 0
        out_of_range = 0
        total_hours = 0
        break_hours = 0
        for rec in records:
            emp_id = rec.get("employee_id")
            if not emp_id:
                skipped += 1
                continue
            try:
                day_str = rec.get("date") or ""
                if not day_str:
                    skipped += 1
                    continue

                # Filter by date range
                month_key = day_str[:7]  # "YYYY-MM"
                if month_key not in valid_months:
                    out_of_range += 1
                    continue

                workable = rec.get("workable", True)

                if not workable:
                    # Break record — add minutes if it's a paid break
                    break_cfg_id = rec.get("time_settings_break_configuration_id")
                    is_paid = False
                    if break_cfg_id and break_configs.get(break_cfg_id, {}).get("paid"):
                        is_paid = True
                    if is_paid:
                        mins = rec.get("minutes") or 0
                        if mins > 0:
                            hours = mins / 60
                            monthly[emp_id][month_key] += hours
                            daily[emp_id][day_str] += hours
                            break_hours += hours
                    continue

                # Normal work shift — require clock_in/clock_out
                clock_in = rec.get("clock_in") or ""
                clock_out = rec.get("clock_out") or ""
                if not clock_in or not clock_out:
                    skipped += 1
                    continue

                # Prefer 'minutes' field (most reliable)
                hours = 0
                minutes = rec.get("minutes")
                if minutes is not None and minutes > 0:
                    hours = minutes / 60
                elif "T" in clock_in:
                    from datetime import datetime as dt
                    t_in = dt.fromisoformat(clock_in.replace("Z", "+00:00"))
                    t_out = dt.fromisoformat(clock_out.replace("Z", "+00:00"))
                    hours = (t_out - t_in).total_seconds() / 3600
                else:
                    h_in, m_in = map(int, clock_in.split(":"))
                    h_out, m_out = map(int, clock_out.split(":"))
                    hours = (h_out * 60 + m_out - h_in * 60 - m_in) / 60

                if hours > 0:
                    monthly[emp_id][month_key] += hours
                    daily[emp_id][day_str] += hours
                    total_hours += hours
            except (ValueError, TypeError):
                skipped += 1

        print(f"    En rango: {total_hours:.1f}h trabajo + {break_hours:.1f}h pausas pagadas, "
              f"fuera de rango: {out_of_range}, saltados: {skipped}")
        print(f"    {len(monthly)} empleados con fichajes en el rango")

        return monthly, daily

    # ── Company Holidays ──────────────────────────────────────────

    def get_company_holidays(self):
        """GET holidays/company-holidays — all company holidays."""
        url = self._url("holidays/company-holidays")
        return self._paginate(url)

    def get_holidays_in_range(self, date_from, date_to):
        """Fetch company holidays in date range.

        Returns: list of {date, name, location_id}
        """
        all_holidays = self.get_company_holidays()
        to_y, to_m = int(date_to[:4]), int(date_to[5:7])
        last_day = calendar.monthrange(to_y, to_m)[1]
        range_start = f"{date_from}-01"
        range_end = f"{date_to}-{last_day:02d}"

        result = []
        for h in all_holidays:
            date = h.get("date") or h.get("start_date") or ""
            if date and range_start <= date <= range_end:
                result.append({
                    "date": date,
                    "name": h.get("name") or h.get("description") or "Festivo",
                    "location_id": h.get("location_id"),
                })

        print(f"    {len(result)} festivos en rango")
        return result

    # ── Leave Types ───────────────────────────────────────────────

    def get_leave_types(self):
        """GET timeoff/leave-types — leave type configurations.

        Returns: {leave_type_id: name}
        """
        url = self._url("timeoff/leave-types")
        types = self._paginate(url)
        result = {}
        for lt in types:
            lt_id = lt.get("id")
            if lt_id:
                result[lt_id] = lt.get("name") or lt.get("description") or "Ausencia"
        print(f"    {len(result)} tipos de ausencia configurados")
        return result

    # ── Leaves / Absences ────────────────────────────────────────

    def get_leaves(self):
        """GET timeoff/leaves — all leave records."""
        url = self._url("timeoff/leaves")
        return self._paginate(url)

    def get_leaves_in_range(self, date_from, date_to):
        """Fetch leaves overlapping the date range.

        Returns: {employee_id -> [{start_date, end_date, leave_type, status}]}
        """
        leave_types = self.get_leave_types()
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
                    # Resolve leave type name: try leave_type_id lookup first
                    lt_id = leave.get("leave_type_id")
                    lt_name = (leave_types.get(lt_id) if lt_id else None) \
                              or leave.get("leave_type_name") \
                              or leave.get("description") \
                              or "Ausencia"
                    result[emp_id].append({
                        "start_date": start,
                        "end_date": end,
                        "leave_type": lt_name,
                        "status": status,
                    })

        # Summary by type
        type_counts = defaultdict(int)
        for entries in result.values():
            for e in entries:
                type_counts[e["leave_type"]] += 1
        if type_counts:
            summary = ", ".join(f"{t}: {c}" for t, c in sorted(type_counts.items()))
            print(f"  {sum(type_counts.values())} ausencias en rango ({summary})")
        else:
            print(f"  0 ausencias en rango")
        return result
