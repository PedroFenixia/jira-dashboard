#!/usr/bin/env python3
"""Informe interactivo de horas reportadas por usuario y mes.

Filtra por grupos JIRA que contengan "reportes".
Desplegable: Usuario → Mes → Tareas.

Usage:
  python report_hours.py
  python report_hours.py --from 2024-01 --to 2026-01
  python report_hours.py --format csv
"""
import argparse
import calendar
import csv
import io
import json
import os
import re
import unicodedata
from collections import defaultdict
from datetime import datetime

from config import Config
from jira_client import JiraClient


def fetch_group_members(client, keyword="reportes"):
    """Fetch all users from JIRA groups containing the keyword."""
    url = f"{client.config.api_url}/groups/picker"
    resp = client._request("GET", url, params={"query": keyword})
    groups = resp.json().get("groups", [])
    print(f"Grupos con '{keyword}': {len(groups)}")

    members = {}  # accountId -> {displayName, groups: []}
    for group in groups:
        group_name = group["name"]
        group_id = group.get("groupId", "")
        print(f"  {group_name}...")

        # Fetch group members
        member_url = f"{client.config.api_url}/group/member"
        params = {"groupId": group_id, "maxResults": 200, "startAt": 0,
                  "includeInactiveUsers": "true"}
        while True:
            data = client._request("GET", member_url, params=params).json()
            for m in data.get("values", []):
                aid = m.get("accountId", "")
                name = m.get("displayName", "Desconocido")
                if aid not in members:
                    members[aid] = {"displayName": name, "groups": []}
                members[aid]["groups"].append(group_name)
            if data.get("isLast", True):
                break
            params["startAt"] += params["maxResults"]

    print(f"  {len(members)} usuarios en total")
    return members


def find_factorial_jira_accounts(client, fact_employees, date_from, date_to,
                                  existing_ids):
    """Find JIRA accounts for Factorial employees not already in report groups.

    Searches ALL Factorial employees (active + terminated) in JIRA by email.
    Skips those already in existing_ids (i.e. already in a 'reportes' group).
    Terminated employees outside the report period are excluded.

    Returns: (accounts, archived_users)
      accounts: dict {accountId: {displayName, groups, archived, terminated_on}}
      archived_users: dict {displayName: {terminated_on, archived}}
    """
    from datetime import date as dt_date

    today = dt_date.today()
    range_start = f"{date_from}-01"
    to_y, to_m = int(date_to[:4]), int(date_to[5:7])
    range_end_day = calendar.monthrange(to_y, to_m)[1]
    range_end = f"{date_to}-{range_end_day:02d}"

    # Build candidates: all active employees + terminated who overlapped period
    candidates = {}
    for email, emp in fact_employees.items():
        term_date = emp.get("terminated_on")
        if term_date:
            start_date = emp.get("start_date") or "2000-01-01"
            if start_date > range_end or term_date < range_start:
                continue  # Terminated outside report period
        candidates[email] = emp

    if not candidates:
        return {}, {}

    print(f"  Buscando {len(candidates)} empleados Factorial en JIRA...")

    found = {}
    archived_users = {}
    already_in_groups = 0
    search_url = f"{client.config.api_url}/user/search"

    for idx, (email, emp) in enumerate(candidates.items()):
        try:
            resp = client._request("GET", search_url,
                                   params={"query": email, "maxResults": 5})
            users = resp.json()
            for u in users:
                aid = u.get("accountId", "")
                u_email = (u.get("emailAddress") or "").strip().lower()
                if aid and u_email == email:
                    if aid in existing_ids:
                        already_in_groups += 1
                        # Still mark as archived if terminated in Factorial
                        term_date = emp.get("terminated_on")
                        if term_date:
                            display_name = u.get("displayName", emp["full_name"])
                            term_dt = dt_date.fromisoformat(term_date)
                            archive_m = term_dt.month + 2
                            archive_y = term_dt.year
                            if archive_m > 12:
                                archive_m -= 12
                                archive_y += 1
                            is_archived = today >= dt_date(archive_y, archive_m, 1)
                            archived_users[display_name] = {
                                "terminated_on": term_date,
                                "archived": is_archived,
                            }
                        break
                    term_date = emp.get("terminated_on")
                    is_archived = False
                    if term_date:
                        term_dt = dt_date.fromisoformat(term_date)
                        archive_m = term_dt.month + 2
                        archive_y = term_dt.year
                        if archive_m > 12:
                            archive_m -= 12
                            archive_y += 1
                        is_archived = today >= dt_date(archive_y, archive_m, 1)
                        group = "Archivados"
                    else:
                        group = "Sin grupo JIRA"

                    display_name = u.get("displayName", emp["full_name"])
                    found[aid] = {
                        "displayName": display_name,
                        "groups": [group],
                        "archived": is_archived,
                        "terminated_on": term_date,
                    }
                    if term_date:
                        archived_users[display_name] = {
                            "terminated_on": term_date,
                            "archived": is_archived,
                        }
                    break
        except Exception:
            pass
        if (idx + 1) % 20 == 0:
            print(f"    {idx + 1}/{len(candidates)} buscados...")

    active_found = sum(1 for v in found.values() if not v.get("terminated_on"))
    term_found = sum(1 for v in found.values() if v.get("terminated_on"))
    print(f"  Ya en grupos: {already_in_groups}, Nuevos: {active_found} activos + "
          f"{term_found} terminados")
    return found, archived_users


def fetch_worklogs(client, date_from, date_to, allowed_account_ids=None):
    """Fetch worklogs with task-level detail.

    Returns: (raw, daily_raw)
      raw: user -> month -> issue_key -> {summary, hours, ...}
      daily_raw: user -> date_str -> hours (for Factorial comparison)
    """
    to_y, to_m = int(date_to[:4]), int(date_to[5:7])
    last_day = calendar.monthrange(to_y, to_m)[1]
    jql = (
        f'worklogDate >= "{date_from}-01" AND worklogDate <= "{date_to}-{last_day:02d}" '
        f"ORDER BY updated ASC"
    )
    fields = ["summary", "project", "worklog", "customfield_10111", "customfield_10094"]
    print(f"Buscando issues con worklogs entre {date_from} y {date_to}...")
    issues = client._search_issues(jql, fields)
    print(f"  {len(issues)} issues encontradas")

    from datetime import timezone
    dt_from = datetime(int(date_from[:4]), int(date_from[5:7]), 1, tzinfo=timezone.utc)
    to_year, to_month = int(date_to[:4]), int(date_to[5:7])
    dt_to = datetime(to_year + (1 if to_month == 12 else 0),
                     1 if to_month == 12 else to_month + 1, 1, tzinfo=timezone.utc)

    # user -> month -> issue_key -> {summary, hours}
    raw = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {"summary": "", "hours": 0.0})))
    # user -> date_str -> hours (daily aggregate for comparison)
    daily_raw = defaultdict(lambda: defaultdict(float))
    total_wl = 0

    for idx, issue in enumerate(issues):
        issue_key = issue["key"]
        summary = issue.get("fields", {}).get("summary", "")
        # Extract Cliente GLOBAL and Neuro360
        cg_field = issue.get("fields", {}).get("customfield_10111")
        cliente_global = cg_field.get("value", "Sin cliente") if isinstance(cg_field, dict) else "Sin cliente"
        n360_field = issue.get("fields", {}).get("customfield_10094")
        neuro360 = n360_field.get("value", "Sin Neuro360") if isinstance(n360_field, dict) else "Sin Neuro360"
        n360_child = ""
        if isinstance(n360_field, dict) and "child" in n360_field:
            n360_child = n360_field["child"].get("value", "")

        worklogs = issue.get("fields", {}).get("worklog", {}).get("worklogs", [])
        wl_total = issue.get("fields", {}).get("worklog", {}).get("total", 0)
        if wl_total > len(worklogs):
            try:
                worklogs = client.get_issue_worklogs(issue_key)
            except Exception:
                pass

        for wl in worklogs:
            started = wl.get("started", "")
            if not started:
                continue
            started_fix = re.sub(r"([+-]\d{2})(\d{2})$", r"\1:\2", started.replace("Z", "+00:00")); wl_date = datetime.fromisoformat(started_fix)
            if wl_date < dt_from or wl_date >= dt_to:
                continue

            author_id = wl.get("author", {}).get("accountId", "")
            author_name = wl.get("author", {}).get("displayName", "Desconocido")

            if allowed_account_ids and author_id not in allowed_account_ids:
                continue

            seconds = wl.get("timeSpentSeconds", 0)
            hours = seconds / 3600
            month_key = f"{wl_date.year}-{wl_date.month:02d}"
            day_key = f"{wl_date.year}-{wl_date.month:02d}-{wl_date.day:02d}"
            entry = raw[author_name][month_key][issue_key]
            entry["summary"] = summary
            entry["hours"] += hours
            entry["cliente_global"] = cliente_global
            entry["neuro360"] = neuro360
            entry["neuro360_child"] = n360_child
            daily_raw[author_name][day_key] += hours
            total_wl += 1

        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(issues)} issues procesadas...")

    print(f"  {total_wl} worklogs procesados")
    return raw, daily_raw


def _search_jql_direct(client, jql, fields):
    """Search using new /search/jql endpoint directly (no legacy fallback)."""
    url = f"{client.config.api_url}/search/jql"
    all_issues = []
    next_token = None
    while True:
        body = {"jql": jql, "fields": fields, "maxResults": 100}
        if next_token:
            body["nextPageToken"] = next_token
        resp = client.session.post(url, json=body)
        if resp.status_code != 200:
            return None, resp.status_code, resp.text[:200]
        data = resp.json()
        all_issues.extend(data.get("issues", []))
        next_token = data.get("nextPageToken")
        if not next_token:
            break
    return all_issues, 200, ""


def fetch_client_changes(client, date_from, date_to, issue_keys_with_client=None):
    """Find issues where Cliente GLOBAL was changed, then fetch changelogs.

    Tries JQL 'changed' operator first. If unsupported, falls back to JQL
    field queries, then to issue keys from worklogs as last resort.
    Returns: {issue_key: [{date, from_val, to_val}, ...]}
    """
    to_y, to_m = int(date_to[:4]), int(date_to[5:7])
    last_day = calendar.monthrange(to_y, to_m)[1]
    wl_filter = (
        f'AND worklogDate >= "{date_from}-01" '
        f'AND worklogDate <= "{date_to}-{last_day:02d}" '
        f'ORDER BY updated ASC'
    )

    print("Buscando issues con cambios de Cliente GLOBAL...")
    issues = None

    # Try JQL 'changed' operator (may not work on all JIRA instances)
    for changed_jql in [
        f'"Cliente GLOBAL" changed {wl_filter}',
        f'cf[10111] changed {wl_filter}',
    ]:
        result, status, err = _search_jql_direct(client, changed_jql, ["summary"])
        if result is not None:
            issues = result
            print(f"  JQL 'changed' OK: {len(issues)} issues")
            break
        else:
            print(f"  JQL 'changed' no soportado (HTTP {status})")

    # Preferred fallback: use issue keys from worklogs that have cliente_global set
    # This is much more targeted than JQL 'is not EMPTY' which returns ~60k issues
    if issues is None and issue_keys_with_client:
        keys = list(issue_keys_with_client)
        print(f"  Usando {len(keys)} issue keys de worklogs con Cliente GLOBAL")
        issues = [{"key": k} for k in keys]

    # Last resort: JQL field query (only if no worklog keys available)
    if issues is None:
        for fallback_jql in [
            f'"Cliente GLOBAL" is not EMPTY {wl_filter}',
            f'customfield_10111 is not EMPTY {wl_filter}',
        ]:
            result, status, err = _search_jql_direct(client, fallback_jql, ["summary"])
            if result is not None and len(result) > 0:
                issues = result
                print(f"  Fallback JQL: {len(issues)} issues con Cliente GLOBAL")
                break
            elif result is not None:
                print(f"  Fallback JQL OK pero 0 resultados")

    if not issues:
        print("  No se encontraron issues para revisar")
        return {}

    # Fetch changelogs and filter for customfield_10111 changes
    MAX_CHANGELOGS = 2000
    to_check = issues[:MAX_CHANGELOGS]
    if len(issues) > MAX_CHANGELOGS:
        print(f"  Limitando a {MAX_CHANGELOGS} de {len(issues)} issues")

    changes = {}
    for idx, issue in enumerate(to_check):
        key = issue["key"]
        try:
            histories = client.get_issue_changelog(key)
        except Exception:
            continue
        issue_changes = []
        for history in histories:
            created = history.get("created", "")
            for item in history.get("items", []):
                if item.get("fieldId") == "customfield_10111" or item.get("field") == "Cliente GLOBAL":
                    from_val = item.get("fromString", "Sin cliente") or "Sin cliente"
                    to_val = item.get("toString", "Sin cliente") or "Sin cliente"
                    # Clean "Parent values: NAME(ID)" format from cascading selects
                    from_val = re.sub(r"^Parent values:\s*", "", from_val)
                    from_val = re.sub(r"\(\d+\)$", "", from_val).strip()
                    to_val = re.sub(r"^Parent values:\s*", "", to_val)
                    to_val = re.sub(r"\(\d+\)$", "", to_val).strip()
                    issue_changes.append({
                        "date": created[:10],
                        "from": from_val,
                        "to": to_val,
                    })
        if issue_changes:
            changes[key] = issue_changes
        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(to_check)} changelogs revisados...")

    print(f"  {len(changes)} issues con cambios de Cliente GLOBAL confirmados")
    return changes


# ── Factorial integration helpers ────────────────────────────────


def fetch_jira_user_emails(client, account_ids):
    """Fetch email for each JIRA account ID."""
    emails = {}
    total = len(account_ids)
    print(f"Obteniendo emails de {total} usuarios JIRA...")
    for idx, aid in enumerate(account_ids):
        try:
            url = f"{client.config.api_url}/user"
            resp = client._request("GET", url, params={"accountId": aid})
            data = resp.json()
            email = (data.get("emailAddress") or "").strip().lower()
            if email:
                emails[aid] = email
        except Exception:
            pass
        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{total} emails obtenidos...")
    print(f"  {len(emails)} emails encontrados de {total} usuarios")
    return emails


def build_employee_match(jira_emails, fact_employees, groups_info):
    """Match JIRA users to Factorial employees by email."""
    matched = {}
    unmatched_jira = []
    matched_fact_emails = set()
    for aid, email in jira_emails.items():
        display_name = groups_info.get(aid, {}).get("displayName", aid)
        if email in fact_employees:
            fact = fact_employees[email]
            matched[display_name] = {
                "factorial_id": fact["id"],
                "factorial_name": fact["full_name"],
            }
            matched_fact_emails.add(email)
        else:
            unmatched_jira.append(display_name)
    unmatched_factorial = [
        info["full_name"]
        for email, info in fact_employees.items()
        if email not in matched_fact_emails
    ]
    print(f"  Match: {len(matched)}, Sin match JIRA: {len(unmatched_jira)}, "
          f"Sin match Factorial: {len(unmatched_factorial)}")
    return matched, unmatched_jira, unmatched_factorial


def build_comparison_data(raw, matched, attendance, months,
                          daily_jira=None, daily_factorial=None):
    """Build JIRA vs Factorial hours comparison with daily detail.

    Structure: {user: {months: {m: {jira, factorial, days: {d: {jira, factorial}}}}}}
    """
    from datetime import date as dt_date
    daily_jira = daily_jira or {}
    daily_factorial = daily_factorial or {}
    comparison = {}

    for display_name, match_info in sorted(matched.items()):
        fact_id = match_info["factorial_id"]
        fact_attendance = attendance.get(fact_id, {})
        fact_daily = daily_factorial.get(fact_id, {})
        jira_daily = daily_jira.get(display_name, {})

        person = {"months": {}}
        total_j, total_f = 0, 0

        for m in months:
            jira_h = 0
            if display_name in raw:
                for info in raw[display_name].get(m, {}).values():
                    jira_h += info["hours"]
            jira_h = round(jira_h, 1)
            fact_h = round(fact_attendance.get(m, 0), 1)

            # Collect daily data for this month
            y, mo = int(m[:4]), int(m[5:7])
            days_in_month = calendar.monthrange(y, mo)[1]
            days = {}
            for d in range(1, days_in_month + 1):
                day_str = f"{m}-{d:02d}"
                dj = round(jira_daily.get(day_str, 0), 1)
                df = round(fact_daily.get(day_str, 0), 1)
                if dj > 0 or df > 0:
                    days[day_str] = {"jira": dj, "factorial": df}

            person["months"][m] = {"jira": jira_h, "factorial": fact_h, "days": days}
            total_j += jira_h
            total_f += fact_h

        if total_j > 0 or total_f > 0:
            comparison[display_name] = person
    return comparison


def build_leaves_data(matched, leaves):
    """Build leaves/absences data per person."""
    result = {}
    for display_name, match_info in sorted(matched.items()):
        fact_id = match_info["factorial_id"]
        emp_leaves = leaves.get(fact_id, [])
        if not emp_leaves:
            continue
        entries = []
        for lv in emp_leaves:
            start = lv["start_date"]
            end = lv["end_date"]
            try:
                from datetime import date
                d1 = date.fromisoformat(start)
                d2 = date.fromisoformat(end)
                days = max(1, (d2 - d1).days + 1)
            except (ValueError, TypeError):
                days = 1
            entries.append({
                "start_date": start,
                "end_date": end,
                "leave_type": lv["leave_type"],
                "status": lv["status"],
                "days": days,
            })
        result[display_name] = entries
    return result


def build_months(date_from, date_to):
    months = []
    y, m = int(date_from[:4]), int(date_from[5:7])
    end_y, end_m = int(date_to[:4]), int(date_to[5:7])
    while (y, m) <= (end_y, end_m):
        months.append(f"{y}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return months


MONTH_NAMES = {
    "01": "Ene", "02": "Feb", "03": "Mar", "04": "Abr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Ago",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dic",
}


def generate_csv(raw, months, output_path):
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["Usuario", "Mes", "Issue", "Resumen", "Horas"])
    for user in sorted(raw.keys()):
        for m in months:
            tasks = raw[user].get(m, {})
            for key, info in sorted(tasks.items()):
                writer.writerow([user, m, key, info["summary"], round(info["hours"], 1)])
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(buf.getvalue())
    return output_path


def generate_html(raw, months, groups_info, date_from, date_to, jira_url, output_path,
                   client_changes=None, comparison_data=None, leaves_data=None,
                   factorial_stats=None, archived_users=None, holidays=None):
    """Generate interactive HTML with tabs: Personal, Neuro360, Cambios Cliente, + Factorial."""

    def _norm(val, default=""):
        """Normalize a string for grouping: NFC unicode, strip, title-case, collapse spaces."""
        v = (val or default).strip()
        v = unicodedata.normalize("NFC", v)
        return " ".join(v.title().split())

    users = sorted(raw.keys())

    # Build personal data: user -> project -> issue_key -> {summary, months}
    personal_data = {}
    for user in users:
        projects = {}
        for m in months:
            tasks = raw[user].get(m, {})
            for key, info in tasks.items():
                proj = key.split("-")[0] if "-" in key else "OTHER"
                if proj not in projects:
                    projects[proj] = {}
                if key not in projects[proj]:
                    projects[proj][key] = {"summary": info["summary"], "months": {}}
                projects[proj][key]["months"][m] = round(
                    projects[proj][key]["months"].get(m, 0) + info["hours"], 1
                )
        if projects:
            personal_data[user] = projects

    # Build issue -> cliente_global mapping for JS filter
    issue_client_map = {}
    client_values_set = set()
    for user in users:
        for m in months:
            tasks = raw[user].get(m, {})
            for key, info in tasks.items():
                cg = info.get("cliente_global", "Sin cliente") or "Sin cliente"
                issue_client_map[key] = cg
                client_values_set.add(cg)
    client_values = sorted(client_values_set)

    # Build neuro data: parent -> child -> issue_key -> {summary, months}
    neuro_data = {}
    for user in users:
        for m in months:
            tasks = raw[user].get(m, {})
            for key, info in tasks.items():
                parent = _norm(info.get("neuro360"), "Sin Neuro360")
                child = _norm(info.get("neuro360_child"), "Sin Subcategoría")
                if parent not in neuro_data:
                    neuro_data[parent] = {}
                if child not in neuro_data[parent]:
                    neuro_data[parent][child] = {}
                if key not in neuro_data[parent][child]:
                    neuro_data[parent][child][key] = {"summary": info["summary"], "months": {}, "users": []}
                neuro_data[parent][child][key]["months"][m] = round(
                    neuro_data[parent][child][key]["months"].get(m, 0) + info["hours"], 1
                )
                if user not in neuro_data[parent][child][key]["users"]:
                    neuro_data[parent][child][key]["users"].append(user)

    # Build changes data: change_label -> issue_key -> {summary, change_date, months}
    changes_data = {}
    if client_changes:
        # Collect all worklogs per issue across all users
        issue_worklogs = {}
        for user in users:
            for m in months:
                tasks = raw[user].get(m, {})
                for key, info in tasks.items():
                    if key in client_changes:
                        if key not in issue_worklogs:
                            issue_worklogs[key] = {"summary": info["summary"], "months": {}}
                        if m not in issue_worklogs[key]["months"]:
                            issue_worklogs[key]["months"][m] = {}
                        issue_worklogs[key]["months"][m][user] = round(
                            issue_worklogs[key]["months"][m].get(user, 0) + info["hours"], 1
                        )
        for key, changes_list in client_changes.items():
            if key not in issue_worklogs:
                continue
            issue_info = issue_worklogs[key]
            for change in changes_list:
                change_date = change["date"]  # "YYYY-MM-DD"
                label = f'{change["from"]} \u2192 {change["to"]}'
                if label not in changes_data:
                    changes_data[label] = {}
                changes_data[label][key] = {
                    "summary": issue_info["summary"],
                    "change_date": change_date,
                    "months": issue_info["months"],
                }

        print(f"  issue_worklogs: {len(issue_worklogs)} issues, changes_data: {sum(len(v) for v in changes_data.values())} entradas en {len(changes_data)} transiciones")

    # Build user -> groups mapping
    user_groups_map = {}
    if groups_info:
        for aid, info in groups_info.items():
            name = info["displayName"]
            if name in personal_data:
                user_groups_map[name] = info["groups"]

    group_names = sorted(set(g for info in groups_info.values() for g in info["groups"])) if groups_info else []

    # Grand total
    grand_total = 0
    for user in users:
        for m in months:
            for info in raw[user].get(m, {}).values():
                grand_total += info["hours"]
    grand_total = round(grand_total, 1)

    # Month selector options
    from_options = ""
    to_options = ""
    for m in months:
        label = MONTH_NAMES.get(m[5:7], m[5:7]) + " " + m[:4]
        from_sel = " selected" if m == months[0] else ""
        to_sel = " selected" if m == months[-1] else ""
        from_options += f'<option value="{m}"{from_sel}>{label}</option>'
        to_options += f'<option value="{m}"{to_sel}>{label}</option>'

    # Year selector options
    year_set = sorted(set(m[:4] for m in months))
    year_options = '<option value="">Todos</option>'
    for y in year_set:
        year_options += f'<option value="{y}">{y}</option>'

    group_items = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{g}" onchange="debounce(buildPersonal)">{g}</label>'
        for g in group_names
    )
    group_items_ch = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{g}" onchange="debounce(buildChanges)">{g}</label>'
        for g in group_names
    )
    group_items_n = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{g}" onchange="debounce(buildNeuro)">{g}</label>'
        for g in group_names
    )
    group_items_cp = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{g}" onchange="debounce(buildComparison)">{g}</label>'
        for g in group_names
    )
    group_items_lv = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{g}" onchange="debounce(buildLeaves)">{g}</label>'
        for g in group_names
    )

    def _esc(v):
        return v.replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")

    client_items_p = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{_esc(c)}" onchange="debounce(buildPersonal)">{_esc(c)}</label>'
        for c in client_values
    )
    client_items_n = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{_esc(c)}" onchange="debounce(buildNeuro)">{_esc(c)}</label>'
        for c in client_values
    )
    client_items_ch = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{_esc(c)}" onchange="debounce(buildChanges)">{_esc(c)}</label>'
        for c in client_values
    )

    holidays_map = {h["date"]: h["name"] for h in (holidays or [])}

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Informe de Horas &mdash; {date_from} a {date_to}</title>
<style>
  :root {{
    --bg: #f8fafc; --card: #fff; --text: #1e293b; --muted: #64748b;
    --border: #e2e8f0; --blue: #3b82f6; --green: #22c55e; --amber: #f59e0b;
    --purple: #8b5cf6;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg); color: var(--text); padding: 24px;
    max-width: 1600px; margin: 0 auto;
  }}
  h1 {{ font-size: 1.5rem; margin-bottom: 4px; }}
  .meta {{ color: var(--muted); font-size: 0.85rem; margin-bottom: 20px; }}

  /* Filters bar */
  .filters {{
    display: flex; gap: 12px; align-items: center; margin-bottom: 16px; flex-wrap: wrap;
    background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px 16px;
  }}
  .filters label {{ font-size: 0.8rem; color: var(--muted); font-weight: 600; }}
  .filters select {{
    padding: 6px 12px; border: 1px solid var(--border); border-radius: 6px;
    font-size: 0.8rem; background: var(--card); color: var(--text); cursor: pointer;
  }}

  /* Summary cards */
  .summary {{
    display: flex; gap: 16px; margin-bottom: 20px; flex-wrap: wrap;
  }}
  .stat {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px 24px; text-align: center;
  }}
  .stat .val {{ font-size: 1.75rem; font-weight: 700; color: var(--blue); }}
  .stat .lbl {{ font-size: 0.75rem; color: var(--muted); text-transform: uppercase; }}

  /* Tabs */
  .tabs {{
    display: flex; gap: 0; margin-bottom: 0; border-bottom: 2px solid var(--border);
  }}
  .tab {{
    padding: 10px 28px; border: none; background: none; cursor: pointer;
    font-size: 0.9rem; color: var(--muted); border-bottom: 2px solid transparent;
    margin-bottom: -2px; transition: all 0.2s;
  }}
  .tab.active {{ color: var(--blue); border-bottom-color: var(--blue); font-weight: 600; }}
  .tab:hover {{ color: var(--text); }}

  /* Tab content */
  .tab-content {{ display: none; padding-top: 16px; }}
  .tab-content.active {{ display: block; }}

  /* Controls per tab */
  .controls {{
    display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; align-items: center;
  }}
  .controls select {{
    padding: 6px 12px; border: 1px solid var(--border); border-radius: 6px;
    font-size: 0.8rem; background: var(--card); color: var(--text);
    cursor: pointer; min-width: 180px;
  }}
  .controls button {{
    padding: 6px 14px; border: 1px solid var(--border); border-radius: 6px;
    background: var(--card); cursor: pointer; font-size: 0.8rem; color: var(--text);
  }}
  .controls button:hover {{ background: #f1f5f9; }}

  /* Multi-select dropdown */
  .multi-select {{
    position: relative; display: inline-block; min-width: 220px;
  }}
  .multi-select .ms-btn {{
    padding: 6px 12px; border: 1px solid var(--border); border-radius: 6px;
    font-size: 0.8rem; background: var(--card); color: var(--text); cursor: pointer;
    width: 100%; text-align: left; display: flex; justify-content: space-between; align-items: center;
  }}
  .multi-select .ms-btn .ms-arrow {{ font-size: 0.6rem; margin-left: 8px; }}
  .multi-select .ms-panel {{
    display: none; position: absolute; top: 100%; left: 0; right: 0;
    background: var(--card); border: 1px solid var(--border); border-radius: 6px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.1); z-index: 20; max-height: 240px;
    overflow-y: auto; margin-top: 2px;
  }}
  .multi-select.open .ms-panel {{ display: block; }}
  .multi-select .ms-item {{
    padding: 6px 12px; font-size: 0.8rem; cursor: pointer; display: flex;
    align-items: center; gap: 8px;
  }}
  .multi-select .ms-item:hover {{ background: #f1f5f9; }}
  .multi-select .ms-item input {{ margin: 0; cursor: pointer; }}

  /* Table */
  .table-wrap {{
    overflow-x: auto; background: var(--card);
    border: 1px solid var(--border); border-radius: 10px;
  }}
  table {{
    width: 100%; border-collapse: collapse; font-size: 0.8rem; white-space: nowrap;
  }}
  th, td {{ padding: 7px 10px; text-align: right; border-bottom: 1px solid var(--border); }}
  th {{ background: #f1f5f9; color: var(--muted); font-size: 0.7rem;
       text-transform: uppercase; position: sticky; top: 0; z-index: 3; }}
  th:first-child {{ text-align: left; position: sticky; left: 0; z-index: 4; }}

  /* Level 0 rows (user / neuro parent) */
  .row-l0 td {{ font-weight: 600; }}
  .row-l0 td:first-child {{ cursor: pointer; text-align: left; position: sticky; left: 0; background: var(--card); z-index: 1; }}
  .row-l0:hover td {{ background: #f0f7ff; }}
  .row-l0 .arrow {{ display: inline-block; width: 16px; transition: transform 0.2s; }}
  .row-l0.open .arrow {{ transform: rotate(90deg); }}
  .row-l0 td.total {{ color: var(--blue); }}

  /* Level 1 rows (project / neuro child) */
  .row-l1 td {{ font-size: 0.78rem; background: #fafbfc; }}
  .row-l1 td:first-child {{ cursor: pointer; text-align: left; padding-left: 32px; position: sticky; left: 0; background: #fafbfc; z-index: 1; }}
  .row-l1:hover td {{ background: #f5f5f5; }}
  .row-l1 .arrow {{ display: inline-block; width: 14px; transition: transform 0.2s; font-size: 0.7rem; }}
  .row-l1.open .arrow {{ transform: rotate(90deg); }}
  .row-l1 td.total {{ color: var(--amber); font-weight: 600; }}

  /* Level 2 rows (task) */
  .row-l2 td {{ font-size: 0.75rem; background: #f8f9fa; color: var(--muted); }}
  .row-l2 td:first-child {{ text-align: left; padding-left: 52px; position: sticky; left: 0; background: #f8f9fa; z-index: 1; }}
  .row-l2 td a {{ color: var(--blue); text-decoration: none; }}
  .row-l2 td a:hover {{ text-decoration: underline; }}

  /* Totals row */
  .row-totals td {{ background: #f1f5f9; border-top: 2px solid var(--border); font-weight: 700; }}
  .row-totals td:first-child {{ text-align: left; position: sticky; left: 0; background: #f1f5f9; z-index: 1; }}
  .row-totals td.grand {{ color: var(--green); font-size: 0.9rem; }}

  /* Year headers */
  .year-row th {{ background: #e8edf5; }}
  .year-th {{ cursor: pointer; text-align: center !important; font-size: 0.78rem; }}
  .year-th:hover {{ background: #dce3ed !important; }}

  .zero {{ color: #cbd5e1; }}
  .archived-name {{ opacity: 0.55; }}
  .badge-archived {{ display: inline-block; font-size: 0.6rem; padding: 1px 6px; border-radius: 8px; margin-left: 6px; font-weight: 500; vertical-align: middle; }}
  .badge-archived.arch {{ background: #e2e8f0; color: #64748b; }}
  .badge-archived.baja {{ background: #fef3c7; color: #92400e; }}
  .archive-toggle {{ display: inline-flex; align-items: center; gap: 4px; font-size: 0.78rem; color: var(--muted); margin-left: 12px; cursor: pointer; }}
  .archive-toggle input {{ cursor: pointer; }}
  .footer {{ text-align: center; color: var(--muted); font-size: 0.7rem; margin-top: 24px; }}
  @media print {{
    body {{ padding: 0; font-size: 0.65rem; }}
    .controls, .filters, .tabs {{ display: none; }}
    .row-l1, .row-l2 {{ display: none !important; }}
  }}
</style>
</head>
<body>
<h1>Informe de Horas Reportadas</h1>
<div class="meta">Generado: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>

<div class="filters">
  <label>A&ntilde;o:</label>
  <select id="yearSelect" onchange="onYearChange()">{year_options}</select>
  <label>Desde:</label>
  <select id="dateFrom" onchange="onDateChange()">{from_options}</select>
  <label>Hasta:</label>
  <select id="dateTo" onchange="onDateChange()">{to_options}</select>
</div>

<div class="summary" id="summaryCards"></div>

<div class="tabs">
  <button class="tab active" data-tab="personal" onclick="switchTab('personal')">Por Persona</button>
  <button class="tab" data-tab="neuro" onclick="switchTab('neuro')">Por Neuro360</button>
  <button class="tab" data-tab="changes" onclick="switchTab('changes')">Cambios Cliente</button>
  <button class="tab" data-tab="compare" onclick="switchTab('compare')" style="display:{'inline-block' if comparison_data else 'none'}">JIRA vs Factorial</button>
  <button class="tab" data-tab="leaves" onclick="switchTab('leaves')" style="display:{'inline-block' if leaves_data else 'none'}">Ausencias</button>
</div>

<div id="tabPersonal" class="tab-content active">
  <div class="controls">
    <div class="multi-select" id="groupSelect">
      <button type="button" class="ms-btn" onclick="document.getElementById('groupSelect').classList.toggle('open')">
        <span id="groupLabel">Todos los grupos</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {group_items}
      </div>
    </div>
    <div class="multi-select" id="clientSelect">
      <button type="button" class="ms-btn" onclick="document.getElementById('clientSelect').classList.toggle('open')">
        <span id="clientLabel">Todos los clientes</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {client_items_p}
      </div>
    </div>
    <button onclick="expandAll('pBody')">Expandir todo</button>
    <button onclick="collapseAll('pBody')">Colapsar todo</button>
    <label class="archive-toggle">
      <input type="checkbox" id="showArchived" onchange="debounce(buildPersonal)"> Mostrar archivados
    </label>
  </div>
  <div class="table-wrap">
    <table>
      <thead id="pHead"></thead>
      <tbody id="pBody"></tbody>
    </table>
  </div>
</div>

<div id="tabNeuro" class="tab-content">
  <div class="controls">
    <div class="multi-select" id="groupSelectN">
      <button type="button" class="ms-btn" onclick="document.getElementById('groupSelectN').classList.toggle('open')">
        <span id="groupLabelN">Todos los grupos</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {group_items_n}
      </div>
    </div>
    <div class="multi-select" id="clientSelectN">
      <button type="button" class="ms-btn" onclick="document.getElementById('clientSelectN').classList.toggle('open')">
        <span id="clientLabelN">Todos los clientes</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {client_items_n}
      </div>
    </div>
    <button onclick="expandAll('nBody')">Expandir todo</button>
    <button onclick="collapseAll('nBody')">Colapsar todo</button>
  </div>
  <div class="table-wrap">
    <table>
      <thead id="nHead"></thead>
      <tbody id="nBody"></tbody>
    </table>
  </div>
</div>

<div id="tabChanges" class="tab-content">
  <div class="controls">
    <div class="multi-select" id="groupSelectCh">
      <button type="button" class="ms-btn" onclick="document.getElementById('groupSelectCh').classList.toggle('open')">
        <span id="groupLabelCh">Todos los grupos</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {group_items_ch}
      </div>
    </div>
    <div class="multi-select" id="clientSelectCh">
      <button type="button" class="ms-btn" onclick="document.getElementById('clientSelectCh').classList.toggle('open')">
        <span id="clientLabelCh">Todos los clientes</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {client_items_ch}
      </div>
    </div>
    <button onclick="expandAll('chBody')">Expandir todo</button>
    <button onclick="collapseAll('chBody')">Colapsar todo</button>
  </div>
  <p style="font-size:0.8rem;color:var(--muted);margin-bottom:12px;">
    Horas reportadas en meses distintos al mes en que se cambi&oacute; el campo Cliente GLOBAL.
  </p>
  <div class="table-wrap">
    <table>
      <thead id="chHead"></thead>
      <tbody id="chBody"></tbody>
    </table>
  </div>
</div>

<div id="tabCompare" class="tab-content">
  <div class="controls">
    <div class="multi-select" id="groupSelectCp">
      <button type="button" class="ms-btn" onclick="document.getElementById('groupSelectCp').classList.toggle('open')">
        <span id="groupLabelCp">Todos los grupos</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {group_items_cp}
      </div>
    </div>
    <button onclick="expandAll('cpBody')">Expandir todo</button>
    <button onclick="collapseAll('cpBody')">Colapsar todo</button>
    <label class="archive-toggle">
      <input type="checkbox" id="showArchivedCp" onchange="debounce(buildComparison)"> Mostrar archivados
    </label>
  </div>
  <p style="font-size:0.8rem;color:var(--muted);margin-bottom:12px;">
    Comparaci&oacute;n de horas JIRA vs fichajes Factorial. Expandir: usuario &rarr; mes &rarr; d&iacute;a.
    <span style="color:var(--green)">&FilledSmallSquare; &lt;10%</span>
    <span style="color:var(--amber)">&FilledSmallSquare; 10-25%</span>
    <span style="color:#ef4444">&FilledSmallSquare; &gt;25%</span>
  </p>
  <div class="table-wrap">
    <table>
      <thead id="cpHead"><tr><th style="text-align:left">Nombre</th><th style="color:var(--blue)">JIRA h</th><th style="color:var(--purple)">Factorial h</th><th>Diff</th><th>%</th></tr></thead>
      <tbody id="cpBody"></tbody>
    </table>
  </div>
</div>

<div id="tabLeaves" class="tab-content">
  <div class="controls">
    <div class="multi-select" id="groupSelectLv">
      <button type="button" class="ms-btn" onclick="document.getElementById('groupSelectLv').classList.toggle('open')">
        <span id="groupLabelLv">Todos los grupos</span><span class="ms-arrow">&#9660;</span>
      </button>
      <div class="ms-panel">
        {group_items_lv}
      </div>
    </div>
    <button onclick="expandAll('lvBody')">Expandir todo</button>
    <button onclick="collapseAll('lvBody')">Colapsar todo</button>
    <label class="archive-toggle">
      <input type="checkbox" id="showArchivedLv" onchange="debounce(buildLeaves)"> Mostrar archivados
    </label>
  </div>
  <div class="table-wrap">
    <table>
      <thead id="lvHead"><tr><th style="text-align:left">Empleado</th><th>Tipo</th><th>Desde</th><th>Hasta</th><th>Estado</th><th>D&iacute;as</th></tr></thead>
      <tbody id="lvBody"></tbody>
    </table>
  </div>
</div>

<div class="footer">Generado por jira-dashboard/report_hours.py</div>

<script>
const PERSONAL = {json.dumps(personal_data, ensure_ascii=False)};
const NEURO = {json.dumps(neuro_data, ensure_ascii=False)};
const CHANGES = {json.dumps(changes_data, ensure_ascii=False)};
const COMPARISON = {json.dumps(comparison_data or dict(), ensure_ascii=False)};
const LEAVES = {json.dumps(leaves_data or dict(), ensure_ascii=False)};
const FSTATS = {json.dumps(factorial_stats or dict(), ensure_ascii=False)};
const ALL_MONTHS = {json.dumps(months)};
const USER_GROUPS = {json.dumps(user_groups_map, ensure_ascii=False)};
const CLIENT_MAP = {json.dumps(issue_client_map, ensure_ascii=False)};
const ARCHIVED = {json.dumps(archived_users or dict(), ensure_ascii=False)};
const HOLIDAYS = {json.dumps(holidays_map, ensure_ascii=False)};
const JIRA = "{jira_url}";
const MNAMES = {json.dumps(MONTH_NAMES)};
const collapsedYears = new Set();

const sortES = (a, b) => a.localeCompare(b, 'es', {{sensitivity: 'base'}});
function fmt(h) {{ return h === 0 ? '<span class="zero">-</span>' : h.toFixed(1); }}
let _dt; function debounce(fn) {{ clearTimeout(_dt); _dt = setTimeout(fn, 80); }}
const LAZY = {{}};

function onYearChange() {{
  const y = document.getElementById('yearSelect').value;
  if (!y) {{
    document.getElementById('dateFrom').value = ALL_MONTHS[0];
    document.getElementById('dateTo').value = ALL_MONTHS[ALL_MONTHS.length - 1];
  }} else {{
    const first = ALL_MONTHS.find(m => m.startsWith(y));
    const last = ALL_MONTHS.filter(m => m.startsWith(y)).pop();
    if (first) document.getElementById('dateFrom').value = first;
    if (last) document.getElementById('dateTo').value = last;
  }}
  onDateChange();
}}

function getVisibleMonths() {{
  const f = document.getElementById('dateFrom').value;
  const t = document.getElementById('dateTo').value;
  return ALL_MONTHS.filter(m => m >= f && m <= t);
}}

function getYearGroups(vm) {{
  const g = {{}};
  vm.forEach(m => {{
    const y = m.slice(0, 4);
    if (!g[y]) g[y] = [];
    g[y].push(m);
  }});
  return g;
}}

function getColumns(vm) {{
  const yg = getYearGroups(vm);
  const cols = [];
  Object.keys(yg).sort().forEach(y => {{
    if (collapsedYears.has(y)) {{
      cols.push({{type: 'year', year: y, months: yg[y]}});
    }} else {{
      yg[y].forEach(m => cols.push({{type: 'month', month: m}}));
    }}
  }});
  return cols;
}}

function colVal(mData, col) {{
  if (col.type === 'year') {{
    let s = 0;
    col.months.forEach(m => s += (mData[m] || 0));
    return s;
  }}
  return mData[col.month] || 0;
}}

function toggleYear(year) {{
  if (collapsedYears.has(year)) collapsedYears.delete(year);
  else collapsedYears.add(year);
  onDateChange();
}}

function buildHeaders(theadId, bodyId) {{
  const vm = getVisibleMonths();
  const cols = getColumns(vm);
  const yg = getYearGroups(vm);
  const years = Object.keys(yg).sort();
  let yr = '<tr class="year-row"><th></th>';
  years.forEach(y => {{
    const cl = collapsedYears.has(y);
    const span = cl ? 1 : yg[y].length;
    const arrow = cl ? '&#9654;' : '&#9660;';
    yr += '<th colspan="' + span + '" class="year-th" onclick="toggleYear(\\'' + y + '\\')">' + arrow + ' ' + y + '</th>';
  }});
  yr += '<th></th></tr>';
  const toggle = bodyId ? ' style="cursor:pointer;user-select:none" onclick="toggleAllRows(\\'' + bodyId + '\\')" title="Expandir / Colapsar todo"' : '';
  const tArrow = bodyId ? '<span class="arrow" style="font-size:0.65rem;margin-right:4px">&#9654;</span> ' : '';
  let mr = '<tr><th' + toggle + '>' + tArrow + 'Nombre</th>';
  cols.forEach(c => {{
    if (c.type === 'year') {{
      mr += '<th>Total</th>';
    }} else {{
      mr += '<th>' + MNAMES[c.month.slice(5)] + '</th>';
    }}
  }});
  mr += '<th>TOTAL</th></tr>';
  document.getElementById(theadId).innerHTML = yr + mr;
}}

function updateSummary() {{
  const vm = getVisibleMonths();
  let totalHours = 0;
  let totalUsers = new Set();
  Object.keys(PERSONAL).forEach(user => {{
    let userH = 0;
    Object.values(PERSONAL[user]).forEach(proj => {{
      Object.values(proj).forEach(t => {{
        vm.forEach(m => {{ userH += (t.months[m] || 0); }});
      }});
    }});
    if (userH > 0) totalUsers.add(user);
    totalHours += userH;
  }});
  document.getElementById('summaryCards').innerHTML =
    '<div class="stat"><div class="val">' + totalUsers.size + '</div><div class="lbl">Usuarios</div></div>' +
    '<div class="stat"><div class="val">' + vm.length + '</div><div class="lbl">Meses</div></div>' +
    '<div class="stat"><div class="val">' + totalHours.toFixed(1) + '</div><div class="lbl">Total horas</div></div>' +
    '<div class="stat"><div class="val">' + (vm.length > 0 ? (totalHours / vm.length).toFixed(1) : '0') + '</div><div class="lbl">Media mensual</div></div>';
}}

function toggle(id) {{
  const row = document.querySelector('tr[data-id="' + id + '"]');
  if (!row) return;
  if (LAZY[id] && !document.querySelector('tr[data-parent="' + id + '"]')) {{
    row.insertAdjacentHTML('afterend', LAZY[id]);
  }}
  row.classList.toggle('open');
  const show = row.classList.contains('open');
  document.querySelectorAll('tr[data-parent="' + id + '"]').forEach(r => {{
    r.style.display = show ? '' : 'none';
    if (!show) {{
      r.classList.remove('open');
      const cid = r.dataset.id;
      if (cid) {{
        document.querySelectorAll('tr[data-parent="' + cid + '"]').forEach(gc => {{
          gc.style.display = 'none';
          gc.classList.remove('open');
        }});
      }}
    }}
  }});
}}

function getSelectedGroups(msId) {{
  msId = msId || 'groupSelect';
  const checks = document.querySelectorAll('#' + msId + ' input[type=checkbox]:checked');
  return Array.from(checks).map(c => c.value);
}}

function updateGroupLabel(msId, lblId) {{
  msId = msId || 'groupSelect';
  lblId = lblId || 'groupLabel';
  const sel = getSelectedGroups(msId);
  const lbl = document.getElementById(lblId);
  if (sel.length === 0) lbl.textContent = 'Todos los grupos';
  else if (sel.length <= 2) lbl.textContent = sel.join(', ');
  else lbl.textContent = sel.length + ' grupos';
}}

function getSelectedClients(msId) {{
  msId = msId || 'clientSelect';
  const checks = document.querySelectorAll('#' + msId + ' input[type=checkbox]:checked');
  return Array.from(checks).map(c => c.value);
}}

function updateClientLabel(msId, lblId) {{
  msId = msId || 'clientSelect';
  lblId = lblId || 'clientLabel';
  const sel = getSelectedClients(msId);
  const lbl = document.getElementById(lblId);
  if (sel.length === 0) lbl.textContent = 'Todos los clientes';
  else if (sel.length <= 2) lbl.textContent = sel.join(', ');
  else lbl.textContent = sel.length + ' clientes';
}}

function issueClientOk(key, selClients) {{
  if (selClients.length === 0) return true;
  return selClients.includes(CLIENT_MAP[key] || 'Sin cliente');
}}

document.addEventListener('click', function(e) {{
  document.querySelectorAll('.multi-select').forEach(function(ms) {{
    if (!ms.contains(e.target)) ms.classList.remove('open');
  }});
}});

function isArchived(user) {{ return !!ARCHIVED[user]; }}
function issueUsersOk(t, selGroups) {{
  if (selGroups.length === 0) return true;
  return (t.users || []).some(u => {{
    const ug = USER_GROUPS[u] || [];
    return selGroups.some(g => ug.includes(g));
  }});
}}
function archBadge(user) {{
  const a = ARCHIVED[user];
  if (!a) return '';
  return a.archived
    ? ' <span class="badge-archived arch">Archivado</span>'
    : ' <span class="badge-archived baja">Baja ' + a.terminated_on + '</span>';
}}

function buildPersonal() {{
  updateGroupLabel();
  updateClientLabel();
  const vm = getVisibleMonths();
  const cols = getColumns(vm);
  const selGroups = getSelectedGroups();
  const selClients = getSelectedClients();
  const showArch = document.getElementById('showArchived') && document.getElementById('showArchived').checked;
  buildHeaders('pHead', 'pBody');
  Object.keys(LAZY).forEach(k => {{ if (k.startsWith('p')) delete LAZY[k]; }});

  const users = Object.keys(PERSONAL).sort(sortES);
  let html = '';
  let rid = 0;
  const mTotals = {{}};
  cols.forEach((c, i) => mTotals[i] = 0);
  let gt = 0;

  users.forEach(user => {{
    if (selGroups.length > 0) {{
      const ug = USER_GROUPS[user] || [];
      if (!selGroups.some(g => ug.includes(g))) return;
    }}
    const projs = PERSONAL[user];
    const uid = 'p' + (rid++);

    const uM = {{}};
    vm.forEach(m => uM[m] = 0);
    let uTotal = 0;
    Object.values(projs).forEach(tasks => {{
      Object.entries(tasks).forEach(([key, t]) => {{
        if (!issueClientOk(key, selClients)) return;
        vm.forEach(m => {{ const h = t.months[m] || 0; uM[m] += h; uTotal += h; }});
      }});
    }});
    if (uTotal === 0) return;

    // Always count in totals, even if archived and hidden
    cols.forEach((c, i) => mTotals[i] += colVal(uM, c));
    gt += uTotal;

    // Skip row rendering for archived users when toggle is off
    if (!showArch && isArchived(user)) return;

    let cells = '';
    cols.forEach(c => cells += '<td>' + fmt(colVal(uM, c)) + '</td>');
    const archCls = ARCHIVED[user] ? ' archived-name' : '';
    html += '<tr class="row-l0' + archCls + '" data-id="' + uid + '">' +
      '<td onclick="toggle(\\'' + uid + '\\')">' +
      '<span class="arrow">&#9654;</span> ' + user + archBadge(user) + '</td>' + cells +
      '<td class="total">' + uTotal.toFixed(1) + '</td></tr>\\n';

    let ch = '';
    Object.keys(projs).sort().forEach(proj => {{
      const pid = 'p' + (rid++);
      const tasks = projs[proj];
      const pM = {{}};
      vm.forEach(m => pM[m] = 0);
      let pTotal = 0;
      Object.entries(tasks).forEach(([key, t]) => {{
        if (!issueClientOk(key, selClients)) return;
        vm.forEach(m => {{ const h = t.months[m] || 0; pM[m] += h; pTotal += h; }});
      }});
      if (pTotal === 0) return;

      let pCells = '';
      cols.forEach(c => pCells += '<td>' + fmt(colVal(pM, c)) + '</td>');
      ch += '<tr class="row-l1" data-id="' + pid + '" data-parent="' + uid + '" style="display:none">' +
        '<td onclick="toggle(\\'' + pid + '\\')">' +
        '<span class="arrow">&#9654;</span> ' + proj + '</td>' + pCells +
        '<td class="total">' + pTotal.toFixed(1) + '</td></tr>\\n';

      let tch = '';
      Object.keys(tasks).sort().forEach(issKey => {{
        if (!issueClientOk(issKey, selClients)) return;
        const t = tasks[issKey];
        const tM = {{}};
        vm.forEach(m => tM[m] = t.months[m] || 0);
        let tT = 0;
        vm.forEach(m => tT += tM[m]);
        if (tT === 0) return;
        let tCells = '';
        cols.forEach(c => tCells += '<td>' + fmt(colVal(tM, c)) + '</td>');
        tch += '<tr class="row-l2" data-parent="' + pid + '" style="display:none">' +
          '<td><a href="' + JIRA + '/browse/' + issKey + '" target="_blank">' + issKey + '</a> ' +
          t.summary.substring(0, 50) + '</td>' + tCells +
          '<td>' + tT.toFixed(1) + '</td></tr>\\n';
      }});
      LAZY[pid] = tch;
    }});
    LAZY[uid] = ch;
  }});

  let tCells = '';
  cols.forEach((c, i) => tCells += '<td class="total">' + mTotals[i].toFixed(1) + '</td>');
  html += '<tr class="row-totals"><td>TOTAL</td>' + tCells +
    '<td class="total grand">' + gt.toFixed(1) + '</td></tr>';
  document.getElementById('pBody').innerHTML = html;
}}

function buildNeuro() {{
  updateGroupLabel('groupSelectN', 'groupLabelN');
  updateClientLabel('clientSelectN', 'clientLabelN');
  const vm = getVisibleMonths();
  const cols = getColumns(vm);
  const selGroups = getSelectedGroups('groupSelectN');
  const selClients = getSelectedClients('clientSelectN');
  buildHeaders('nHead', 'nBody');
  Object.keys(LAZY).forEach(k => {{ if (k.startsWith('n')) delete LAZY[k]; }});

  const parents = Object.keys(NEURO).sort(sortES);
  let html = '';
  let rid = 0;
  const mTotals = {{}};
  cols.forEach((c, i) => mTotals[i] = 0);
  let gt = 0;

  parents.forEach(parent => {{
    const children = NEURO[parent];
    const uid = 'n' + (rid++);

    const uM = {{}};
    vm.forEach(m => uM[m] = 0);
    let uTotal = 0;
    Object.values(children).forEach(tasks => {{
      Object.entries(tasks).forEach(([key, t]) => {{
        if (!issueClientOk(key, selClients)) return;
        if (!issueUsersOk(t, selGroups)) return;
        vm.forEach(m => {{ const h = t.months[m] || 0; uM[m] += h; uTotal += h; }});
      }});
    }});
    if (uTotal === 0) return;

    let cells = '';
    cols.forEach((c, i) => {{ const v = colVal(uM, c); mTotals[i] += v; cells += '<td>' + fmt(v) + '</td>'; }});
    gt += uTotal;
    html += '<tr class="row-l0" data-id="' + uid + '">' +
      '<td onclick="toggle(\\'' + uid + '\\')">' +
      '<span class="arrow">&#9654;</span> ' + parent + '</td>' + cells +
      '<td class="total">' + uTotal.toFixed(1) + '</td></tr>\\n';

    let ch = '';
    Object.keys(children).sort(sortES).forEach(child => {{
      const cid = 'n' + (rid++);
      const tasks = children[child];
      const cM = {{}};
      vm.forEach(m => cM[m] = 0);
      let cTotal = 0;
      Object.entries(tasks).forEach(([key, t]) => {{
        if (!issueClientOk(key, selClients)) return;
        if (!issueUsersOk(t, selGroups)) return;
        vm.forEach(m => {{ const h = t.months[m] || 0; cM[m] += h; cTotal += h; }});
      }});
      if (cTotal === 0) return;

      let cCells = '';
      cols.forEach(c => cCells += '<td>' + fmt(colVal(cM, c)) + '</td>');
      ch += '<tr class="row-l1" data-id="' + cid + '" data-parent="' + uid + '" style="display:none">' +
        '<td onclick="toggle(\\'' + cid + '\\')">' +
        '<span class="arrow">&#9654;</span> ' + child + '</td>' + cCells +
        '<td class="total">' + cTotal.toFixed(1) + '</td></tr>\\n';

      let tch = '';
      Object.keys(tasks).sort().forEach(issKey => {{
        if (!issueClientOk(issKey, selClients)) return;
        const t = tasks[issKey];
        if (!issueUsersOk(t, selGroups)) return;
        const tM = {{}};
        vm.forEach(m => tM[m] = t.months[m] || 0);
        let tT = 0;
        vm.forEach(m => tT += tM[m]);
        if (tT === 0) return;
        let tCells = '';
        cols.forEach(c => tCells += '<td>' + fmt(colVal(tM, c)) + '</td>');
        tch += '<tr class="row-l2" data-parent="' + cid + '" style="display:none">' +
          '<td><a href="' + JIRA + '/browse/' + issKey + '" target="_blank">' + issKey + '</a> ' +
          t.summary.substring(0, 50) + '</td>' + tCells +
          '<td>' + tT.toFixed(1) + '</td></tr>\\n';
      }});
      LAZY[cid] = tch;
    }});
    LAZY[uid] = ch;
  }});

  let tCells = '';
  cols.forEach((c, i) => tCells += '<td class="total">' + mTotals[i].toFixed(1) + '</td>');
  html += '<tr class="row-totals"><td>TOTAL</td>' + tCells +
    '<td class="total grand">' + gt.toFixed(1) + '</td></tr>';
  document.getElementById('nBody').innerHTML = html;
}}

function buildChanges() {{
  updateGroupLabel('groupSelectCh', 'groupLabelCh');
  updateClientLabel('clientSelectCh', 'clientLabelCh');
  const vm = getVisibleMonths();
  const cols = getColumns(vm);
  const selGroups = getSelectedGroups('groupSelectCh');
  const selClients = getSelectedClients('clientSelectCh');
  buildHeaders('chHead', 'chBody');
  Object.keys(LAZY).forEach(k => {{ if (k.startsWith('ch')) delete LAZY[k]; }});

  function isUserOk(u) {{
    if (selGroups.length === 0) return true;
    const ug = USER_GROUPS[u] || [];
    return selGroups.some(g => ug.includes(g));
  }}
  function sumM(mEntry) {{
    if (!mEntry) return 0;
    let s = 0;
    Object.keys(mEntry).forEach(u => {{ if (isUserOk(u)) s += mEntry[u]; }});
    return s;
  }}

  const labels = Object.keys(CHANGES).sort(sortES);
  let html = '';
  let rid = 0;
  const mTotals = {{}};
  cols.forEach((c, i) => mTotals[i] = 0);
  let gt = 0;

  labels.forEach(label => {{
    const tasks = CHANGES[label];
    const lid = 'ch' + (rid++);

    const lM = {{}};
    vm.forEach(m => lM[m] = 0);
    let lTotal = 0;
    Object.entries(tasks).forEach(([key, t]) => {{
      if (!issueClientOk(key, selClients)) return;
      vm.forEach(m => {{ const h = sumM(t.months[m]); lM[m] += h; lTotal += h; }});
    }});
    if (lTotal === 0) return;

    let cells = '';
    cols.forEach((c, i) => {{ const v = colVal(lM, c); mTotals[i] += v; cells += '<td>' + fmt(v) + '</td>'; }});
    gt += lTotal;
    html += '<tr class="row-l0" data-id="' + lid + '">' +
      '<td onclick="toggle(\\'' + lid + '\\')">' +
      '<span class="arrow">&#9654;</span> ' + label + '</td>' + cells +
      '<td class="total">' + lTotal.toFixed(1) + '</td></tr>\\n';

    let ch = '';
    Object.keys(tasks).sort().forEach(issKey => {{
      if (!issueClientOk(issKey, selClients)) return;
      const t = tasks[issKey];
      const tM = {{}};
      vm.forEach(m => tM[m] = sumM(t.months[m]));
      let tT = 0;
      vm.forEach(m => tT += tM[m]);
      if (tT === 0) return;
      let tCells = '';
      cols.forEach(c => tCells += '<td>' + fmt(colVal(tM, c)) + '</td>');
      ch += '<tr class="row-l1" data-parent="' + lid + '" style="display:none">' +
        '<td><a href="' + JIRA + '/browse/' + issKey + '" target="_blank">' + issKey + '</a> ' +
        t.summary.substring(0, 50) + ' <small style="color:#94a3b8">(' + t.change_date + ')</small></td>' + tCells +
        '<td class="total">' + tT.toFixed(1) + '</td></tr>\\n';
    }});
    LAZY[lid] = ch;
  }});

  let tCells = '';
  cols.forEach((c, i) => tCells += '<td class="total">' + mTotals[i].toFixed(1) + '</td>');
  html += '<tr class="row-totals"><td>TOTAL</td>' + tCells +
    '<td class="total grand">' + gt.toFixed(1) + '</td></tr>';
  document.getElementById('chBody').innerHTML = html;
}}

function cpDiffCell(j, f) {{
  const diff = j - f;
  const pct = f > 0 ? Math.abs(diff / f * 100) : (j > 0 ? 100 : 0);
  const bg = pct <= 10 ? '' : pct <= 25 ? 'background:#fef3c7;' : 'background:#fecaca;';
  const sign = diff > 0 ? '+' : '';
  return '<td style="' + bg + '">' + sign + diff.toFixed(1) + '</td><td style="' + bg + '">' + pct.toFixed(0) + '%</td>';
}}

const DAYNAMES = ['Dom','Lun','Mar','Mi\u00e9','Jue','Vie','S\u00e1b'];

function buildComparison() {{
  updateGroupLabel('groupSelectCp', 'groupLabelCp');
  const vm = getVisibleMonths();
  const selGroups = getSelectedGroups('groupSelectCp');
  const showArch = document.getElementById('showArchivedCp') && document.getElementById('showArchivedCp').checked;
  Object.keys(LAZY).forEach(k => {{ if (k.startsWith('cp')) delete LAZY[k]; }});

  const users = Object.keys(COMPARISON).sort(sortES);
  let html = '';
  let rid = 0;
  let tJ = 0, tF = 0;

  users.forEach(user => {{
    if (selGroups.length > 0) {{
      const ug = USER_GROUPS[user] || [];
      if (!selGroups.some(g => ug.includes(g))) return;
    }}
    const p = COMPARISON[user];
    let uJ = 0, uF = 0;
    vm.forEach(m => {{
      const d = p.months[m] || {{jira:0,factorial:0}};
      uJ += d.jira; uF += d.factorial;
    }});
    if (uJ === 0 && uF === 0) return;
    tJ += uJ; tF += uF;
    if (!showArch && isArchived(user)) return;
    const uid = 'cp' + (rid++);

    const archCls = ARCHIVED[user] ? ' archived-name' : '';
    html += '<tr class="row-l0' + archCls + '" data-id="' + uid + '">' +
      '<td onclick="toggle(\\'' + uid + '\\')" style="text-align:left">' +
      '<span class="arrow">&#9654;</span> ' + user + archBadge(user) + '</td>' +
      '<td style="color:var(--blue)">' + uJ.toFixed(1) + '</td>' +
      '<td style="color:var(--purple)">' + uF.toFixed(1) + '</td>' +
      cpDiffCell(uJ, uF) + '</tr>\\n';

    let mch = '';
    vm.forEach(m => {{
      const md = p.months[m] || {{jira:0,factorial:0,days:{{}}}};
      if (md.jira === 0 && md.factorial === 0) return;
      const mid = 'cp' + (rid++);
      const mLabel = MNAMES[m.slice(5)] + ' ' + m.slice(0,4);

      mch += '<tr class="row-l1" data-id="' + mid + '" data-parent="' + uid + '" style="display:none">' +
        '<td onclick="toggle(\\'' + mid + '\\')" style="padding-left:24px;text-align:left">' +
        '<span class="arrow">&#9654;</span> ' + mLabel + '</td>' +
        '<td style="color:var(--blue)">' + md.jira.toFixed(1) + '</td>' +
        '<td style="color:var(--purple)">' + md.factorial.toFixed(1) + '</td>' +
        cpDiffCell(md.jira, md.factorial) + '</tr>\\n';

      let dch = '';
      const days = md.days || {{}};
      Object.keys(days).sort().forEach(day => {{
        const dd = days[day];
        const dt = new Date(day + 'T12:00:00');
        const dayName = DAYNAMES[dt.getDay()];
        const hol = HOLIDAYS[day];
        const holBadge = hol ? ' <span style="font-size:0.6rem;background:#dbeafe;color:#1e40af;padding:1px 5px;border-radius:6px">' + hol + '</span>' : '';
        const dayLabel = dayName + ' ' + parseInt(day.slice(8));
        dch += '<tr class="row-l2" data-parent="' + mid + '" style="display:none">' +
          '<td style="padding-left:48px;text-align:left">' + dayLabel + holBadge + '</td>' +
          '<td style="color:var(--blue)">' + dd.jira.toFixed(1) + '</td>' +
          '<td style="color:var(--purple)">' + dd.factorial.toFixed(1) + '</td>' +
          cpDiffCell(dd.jira, dd.factorial) + '</tr>\\n';
      }});
      LAZY[mid] = dch;
    }});
    LAZY[uid] = mch;
  }});

  html += '<tr class="row-totals"><td>TOTAL</td>' +
    '<td class="total" style="color:var(--blue)">' + tJ.toFixed(1) + '</td>' +
    '<td class="total" style="color:var(--purple)">' + tF.toFixed(1) + '</td>' +
    cpDiffCell(tJ, tF) + '</tr>';
  document.getElementById('cpBody').innerHTML = html;
}}

function buildLeaves() {{
  updateGroupLabel('groupSelectLv', 'groupLabelLv');
  const selGroups = getSelectedGroups('groupSelectLv');
  const showArch = document.getElementById('showArchivedLv') && document.getElementById('showArchivedLv').checked;
  Object.keys(LAZY).forEach(k => {{ if (k.startsWith('lv')) delete LAZY[k]; }});
  const users = Object.keys(LEAVES).sort(sortES);
  let html = '';
  let rid = 0;
  let totalDays = 0;

  users.forEach(user => {{
    if (selGroups.length > 0) {{
      const ug = USER_GROUPS[user] || [];
      if (!selGroups.some(g => ug.includes(g))) return;
    }}
    const entries = LEAVES[user];
    const uid = 'lv' + (rid++);
    let uDays = 0;
    const typeDays = {{}};

    // Group entries by month (YYYY-MM from start_date)
    const byMonth = {{}};
    entries.forEach(e => {{
      uDays += e.days;
      typeDays[e.leave_type] = (typeDays[e.leave_type] || 0) + e.days;
      const mk = e.start_date ? e.start_date.substring(0, 7) : '0000-00';
      if (!byMonth[mk]) byMonth[mk] = [];
      byMonth[mk].push(e);
    }});
    totalDays += uDays;
    if (!showArch && isArchived(user)) return;
    const typeSummary = Object.entries(typeDays).map(([t, d]) => t + ' (' + d + 'd)').join(', ');

    const archCls = ARCHIVED[user] ? ' archived-name' : '';
    html += '<tr class="row-l0' + archCls + '" data-id="' + uid + '">' +
      '<td onclick="toggle(\\'' + uid + '\\')" style="text-align:left;position:sticky;left:0;background:var(--card);z-index:1">' +
      '<span class="arrow">&#9654;</span> ' + user + archBadge(user) + '</td>' +
      '<td style="text-align:left;font-size:0.75rem;color:var(--muted)">' + typeSummary + '</td><td></td><td></td><td></td><td class="total">' + uDays + '</td></tr>\\n';

    // Month-level rows (L1) with detail rows (L2) inside
    let mhtml = '';
    const sortedMonths = Object.keys(byMonth).sort();
    sortedMonths.forEach(mk => {{
      const mEntries = byMonth[mk];
      const mid = 'lv' + (rid++);
      let mDays = 0;
      const mTypeDays = {{}};
      mEntries.forEach(e => {{
        mDays += e.days;
        mTypeDays[e.leave_type] = (mTypeDays[e.leave_type] || 0) + e.days;
      }});
      const mTypeSummary = Object.entries(mTypeDays).map(([t, d]) => t + ' (' + d + 'd)').join(', ');
      const mLabel = MNAMES[mk.substring(5, 7)] + ' ' + mk.substring(0, 4);

      mhtml += '<tr class="row-l1" data-parent="' + uid + '" data-id="' + mid + '" style="display:none">' +
        '<td onclick="toggle(\\'' + mid + '\\')" style="padding-left:24px;text-align:left;position:sticky;left:0;background:#fafbfc;z-index:1">' +
        '<span class="arrow">&#9654;</span> ' + mLabel + '</td>' +
        '<td style="text-align:left;font-size:0.75rem;color:var(--muted)">' + mTypeSummary + '</td><td></td><td></td><td></td><td class="total">' + mDays + '</td></tr>\\n';

      // Detail rows (L2)
      let dhtml = '';
      mEntries.forEach(e => {{
        dhtml += '<tr class="row-l2" data-parent="' + mid + '" style="display:none">' +
          '<td style="padding-left:48px;text-align:left;position:sticky;left:0;background:#f4f5f7;z-index:1">&nbsp;</td>' +
          '<td style="text-align:left">' + e.leave_type + '</td>' +
          '<td>' + e.start_date + '</td>' +
          '<td>' + e.end_date + '</td>' +
          '<td>' + e.status + '</td>' +
          '<td>' + e.days + '</td></tr>\\n';
      }});
      LAZY[mid] = dhtml;
    }});
    LAZY[uid] = mhtml;
  }});

  html += '<tr class="row-totals"><td>TOTAL</td><td></td><td></td><td></td><td></td>' +
    '<td class="total grand">' + totalDays + '</td></tr>';
  document.getElementById('lvBody').innerHTML = html;
}}

function switchTab(tab) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelector('.tab[data-tab="' + tab + '"]').classList.add('active');
  document.getElementById('tabPersonal').classList.toggle('active', tab === 'personal');
  document.getElementById('tabNeuro').classList.toggle('active', tab === 'neuro');
  document.getElementById('tabChanges').classList.toggle('active', tab === 'changes');
  document.getElementById('tabCompare').classList.toggle('active', tab === 'compare');
  document.getElementById('tabLeaves').classList.toggle('active', tab === 'leaves');
  if (tab === 'personal') buildPersonal();
  else if (tab === 'neuro') buildNeuro();
  else if (tab === 'changes') buildChanges();
  else if (tab === 'compare') buildComparison();
  else if (tab === 'leaves') buildLeaves();
}}

function toggleAllRows(bodyId) {{
  const el = document.getElementById(bodyId);
  const hasOpen = el.querySelector('.row-l0.open');
  if (hasOpen) collapseAll(bodyId);
  else expandAll(bodyId);
}}

function injectAll(bodyId) {{
  const el = document.getElementById(bodyId);
  el.querySelectorAll('[data-id]').forEach(r => {{
    const id = r.dataset.id;
    if (LAZY[id] && !el.querySelector('tr[data-parent="' + id + '"]')) {{
      r.insertAdjacentHTML('afterend', LAZY[id]);
    }}
  }});
}}

function expandAll(bodyId) {{
  injectAll(bodyId);
  const el = document.getElementById(bodyId);
  el.querySelectorAll('.row-l0, .row-l1').forEach(r => r.classList.add('open'));
  el.querySelectorAll('tr[data-parent]').forEach(r => r.style.display = '');
}}

function collapseAll(bodyId) {{
  const el = document.getElementById(bodyId);
  el.querySelectorAll('.row-l0, .row-l1').forEach(r => r.classList.remove('open'));
  el.querySelectorAll('tr[data-parent]').forEach(r => r.style.display = 'none');
}}

function onDateChange() {{
  debounce(function() {{
    updateSummary();
    const active = document.querySelector('.tab.active').dataset.tab;
    if (active === 'personal') buildPersonal();
    else if (active === 'neuro') buildNeuro();
    else if (active === 'changes') buildChanges();
    else if (active === 'compare') buildComparison();
    else if (active === 'leaves') buildLeaves();
  }});
}}

updateSummary();
buildPersonal();
</script>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Informe de horas JIRA por usuario y mes")
    current_month = datetime.now().strftime("%Y-%m")
    parser.add_argument("--from", dest="date_from", default="2024-01",
                        help="Mes inicio YYYY-MM (default: 2024-01)")
    parser.add_argument("--to", dest="date_to", default=current_month,
                        help=f"Mes fin YYYY-MM (default: {current_month})")
    parser.add_argument("--format", choices=["html", "csv"], default="html",
                        help="Formato de salida (default: html)")
    parser.add_argument("--no-group-filter", action="store_true",
                        help="No filtrar por grupo, incluir todos los usuarios")
    parser.add_argument("-o", "--output", help="Archivo de salida")
    args = parser.parse_args()

    config = Config()
    client = JiraClient(config)

    # Fetch group members
    allowed_ids = None
    groups_info = {}
    if not args.no_group_filter:
        print("Obteniendo miembros de grupos 'reportes'...")
        groups_info = fetch_group_members(client, keyword="reportes")
        allowed_ids = set(groups_info.keys())
        if not allowed_ids:
            print("Aviso: No se encontraron usuarios en grupos 'reportes'. Usando todos.")
            allowed_ids = None

    # Early Factorial: fetch employees and find their JIRA accounts
    all_fact_employees = {}
    archived_users = {}  # displayName -> {terminated_on, archived}
    if config.factorial_enabled and allowed_ids:
        try:
            from factorial_client import FactorialClient
            print("\n── Factorial: empleados ──────────────────")
            for api_key in config.factorial_api_keys:
                fc_config = type(config).__new__(type(config))
                fc_config.factorial_api_key = api_key
                fact_client = FactorialClient(fc_config)
                emp_map = fact_client.get_employees_map()
                all_fact_employees.update(emp_map)
            print(f"  Total: {len(all_fact_employees)} empleados Factorial")

            # Find ALL Factorial employees in JIRA (active + terminated)
            new_accounts, new_archived = find_factorial_jira_accounts(
                client, all_fact_employees, args.date_from, args.date_to,
                allowed_ids
            )
            for aid, info in new_accounts.items():
                groups_info[aid] = info
                allowed_ids.add(aid)
            archived_users.update(new_archived)
            if new_accounts:
                print(f"  {len(new_accounts)} empleados Factorial añadidos al informe")

            # Name-based fallback: match JIRA group members to terminated
            # Factorial employees (catches deactivated JIRA accounts not
            # found by email search)
            def _norm(name):
                n = unicodedata.normalize('NFD', name)
                n = ''.join(c for c in n if unicodedata.category(c) != 'Mn')
                return ' '.join(n.lower().split())

            fact_term_by_name = {}
            for email, emp in all_fact_employees.items():
                if emp.get("terminated_on"):
                    fact_term_by_name[_norm(emp["full_name"])] = emp

            name_matched = 0
            already_known = {_norm(k) for k in archived_users}
            from datetime import date as dt_date
            today = dt_date.today()
            for aid, info in groups_info.items():
                dn = info["displayName"]
                nn = _norm(dn)
                if nn in already_known:
                    continue
                if nn in fact_term_by_name:
                    emp = fact_term_by_name[nn]
                    term_date = emp["terminated_on"]
                    term_dt = dt_date.fromisoformat(term_date)
                    archive_m = term_dt.month + 2
                    archive_y = term_dt.year
                    if archive_m > 12:
                        archive_m -= 12
                        archive_y += 1
                    is_archived = today >= dt_date(archive_y, archive_m, 1)
                    archived_users[dn] = {
                        "terminated_on": term_date,
                        "archived": is_archived,
                    }
                    name_matched += 1
            if name_matched:
                print(f"  {name_matched} archivados detectados por nombre")
            print(f"  Total archivados: {len(archived_users)}")
        except Exception as e:
            import traceback
            print(f"Error buscando empleados Factorial en JIRA: {e}")
            traceback.print_exc()

    months = build_months(args.date_from, args.date_to)
    raw, daily_raw = fetch_worklogs(client, args.date_from, args.date_to, allowed_ids)

    if not raw:
        print("Aviso: No se encontraron worklogs en el rango de fechas. Generando informe vacío.")

    # Collect issue keys with cliente_global set (for fallback in fetch_client_changes)
    keys_with_client = set()
    for user_data in raw.values():
        for month_data in user_data.values():
            for key, info in month_data.items():
                if info.get("cliente_global", "Sin cliente") != "Sin cliente":
                    keys_with_client.add(key)

    # Fetch client field changes (uses JQL server-side filter)
    client_changes = fetch_client_changes(
        client, args.date_from, args.date_to,
        issue_keys_with_client=keys_with_client
    )

    # Factorial integration: attendance, leaves, matching
    comparison_data = None
    leaves_data = None
    factorial_stats = None
    all_holidays = []
    if config.factorial_enabled:
        try:
            from factorial_client import FactorialClient
            print("\n── Factorial HR: fichajes y ausencias ──────")

            # If employees weren't fetched early, fetch now
            if not all_fact_employees:
                for api_key in config.factorial_api_keys:
                    fc_config = type(config).__new__(type(config))
                    fc_config.factorial_api_key = api_key
                    fact_client = FactorialClient(fc_config)
                    emp_map = fact_client.get_employees_map()
                    all_fact_employees.update(emp_map)
                print(f"  Total: {len(all_fact_employees)} empleados Factorial")

            all_attendance = defaultdict(lambda: defaultdict(float))
            all_attendance_daily = defaultdict(lambda: defaultdict(float))
            all_leaves_raw = defaultdict(list)

            for key_idx, api_key in enumerate(config.factorial_api_keys, 1):
                print(f"\n  Empresa Factorial {key_idx}/{len(config.factorial_api_keys)}:")
                fc_config = type(config).__new__(type(config))
                fc_config.factorial_api_key = api_key
                fact_client = FactorialClient(fc_config)

                print(f"  Obteniendo fichajes...")
                att_monthly, att_daily = fact_client.get_attendance_range(args.date_from, args.date_to)
                for emp_id, months_data in att_monthly.items():
                    for m, hours in months_data.items():
                        all_attendance[emp_id][m] += hours
                for emp_id, days_data in att_daily.items():
                    for d, hours in days_data.items():
                        all_attendance_daily[emp_id][d] += hours

                print(f"  Obteniendo ausencias...")
                lvs = fact_client.get_leaves_in_range(args.date_from, args.date_to)
                for emp_id, leaves_list in lvs.items():
                    all_leaves_raw[emp_id].extend(leaves_list)

            # Fetch company holidays (use last client)
            all_holidays = []
            try:
                print(f"  Obteniendo festivos...")
                all_holidays = fact_client.get_holidays_in_range(args.date_from, args.date_to)
            except Exception:
                pass

            jira_emails = fetch_jira_user_emails(client, set(groups_info.keys()))

            matched, unmatched_j, unmatched_f = build_employee_match(
                jira_emails, all_fact_employees, groups_info
            )
            factorial_stats = {
                "matched": len(matched),
                "unmatched_jira": len(unmatched_j),
                "unmatched_factorial": len(unmatched_f),
            }

            comparison_data = build_comparison_data(
                raw, matched, all_attendance, months,
                daily_jira=daily_raw, daily_factorial=all_attendance_daily
            )
            leaves_data = build_leaves_data(matched, all_leaves_raw)
            print(f"  {len(comparison_data)} personas en comparación, "
                  f"{len(leaves_data)} con ausencias")
        except Exception as e:
            import traceback
            print(f"\nError en Factorial (continuando sin datos Factorial): {e}")
            traceback.print_exc()

    if args.format == "csv":
        out = args.output or f"output/horas_{args.date_from}_{args.date_to}.csv"
        path = generate_csv(raw, months, out)
    else:
        out = args.output or f"output/horas_{args.date_from}_{args.date_to}.html"
        path = generate_html(raw, months, groups_info, args.date_from, args.date_to,
                             config.jira_url, out, client_changes=client_changes,
                             comparison_data=comparison_data, leaves_data=leaves_data,
                             factorial_stats=factorial_stats,
                             archived_users=archived_users,
                             holidays=all_holidays)

    abs_path = os.path.abspath(path)
    print(f"\nInforme generado: {abs_path}")
    print(f"Abrir: file://{abs_path}")


if __name__ == "__main__":
    main()
