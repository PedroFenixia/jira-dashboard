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


def fetch_worklogs(client, date_from, date_to, allowed_account_ids=None):
    """Fetch worklogs with task-level detail.

    Returns: user -> month -> [{key, summary, hours}]
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
            month_key = f"{wl_date.year}-{wl_date.month:02d}"
            entry = raw[author_name][month_key][issue_key]
            entry["summary"] = summary
            entry["hours"] += seconds / 3600
            entry["cliente_global"] = cliente_global
            entry["neuro360"] = neuro360
            entry["neuro360_child"] = n360_child
            total_wl += 1

        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(issues)} issues procesadas...")

    print(f"  {total_wl} worklogs procesados")
    return raw


def fetch_client_changes(client, date_from, date_to):
    """Find issues where Cliente GLOBAL was changed via JQL, then fetch changelogs.

    Uses JQL 'changed' operator to filter server-side, avoiding per-issue API calls.
    Returns: {issue_key: [{date, from_val, to_val}, ...]}
    """
    to_y, to_m = int(date_to[:4]), int(date_to[5:7])
    last_day = calendar.monthrange(to_y, to_m)[1]

    jql = (
        f'"Cliente GLOBAL" changed '
        f'AND worklogDate >= "{date_from}-01" '
        f'AND worklogDate <= "{date_to}-{last_day:02d}" '
        f'ORDER BY updated ASC'
    )
    print("Buscando issues con cambios de Cliente GLOBAL (JQL)...")
    try:
        issues = client._search_issues(jql, fields=["summary"])
    except Exception:
        # Fallback: try cf[10111] syntax
        try:
            jql_alt = jql.replace('"Cliente GLOBAL" changed', 'cf[10111] changed')
            issues = client._search_issues(jql_alt, fields=["summary"])
        except Exception:
            print("  No se pudo buscar por cambios de campo. Saltando.")
            return {}

    print(f"  {len(issues)} issues con cambios encontradas")

    changes = {}
    for idx, issue in enumerate(issues):
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
                    issue_changes.append({
                        "date": created[:10],
                        "from": from_val,
                        "to": to_val,
                    })
        if issue_changes:
            changes[key] = issue_changes
        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(issues)} changelogs revisados...")

    print(f"  {len(changes)} issues con cambios de Cliente GLOBAL confirmados")
    return changes


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
                   client_changes=None):
    """Generate interactive HTML with tabs: Personal, Neuro360, Cambios Cliente."""
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

    # Build neuro data: parent -> child -> issue_key -> {summary, months}
    neuro_data = {}
    for user in users:
        for m in months:
            tasks = raw[user].get(m, {})
            for key, info in tasks.items():
                parent = info.get("neuro360", "Sin Neuro360")
                child = info.get("neuro360_child", "") or "Sin subcategoría"
                if parent not in neuro_data:
                    neuro_data[parent] = {}
                if child not in neuro_data[parent]:
                    neuro_data[parent][child] = {}
                if key not in neuro_data[parent][child]:
                    neuro_data[parent][child][key] = {"summary": info["summary"], "months": {}}
                neuro_data[parent][child][key]["months"][m] = round(
                    neuro_data[parent][child][key]["months"].get(m, 0) + info["hours"], 1
                )

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
                        issue_worklogs[key]["months"][m] = round(
                            issue_worklogs[key]["months"].get(m, 0) + info["hours"], 1
                        )
        for key, changes_list in client_changes.items():
            if key not in issue_worklogs:
                continue
            issue_info = issue_worklogs[key]
            change_months = set(c["date"][:7] for c in changes_list)
            # Only include months different from change months
            filtered_months = {m: h for m, h in issue_info["months"].items()
                               if m not in change_months}
            if not filtered_months:
                continue
            # Use each change transition as a label
            for change in changes_list:
                label = f'{change["from"]} \u2192 {change["to"]}'
                if label not in changes_data:
                    changes_data[label] = {}
                changes_data[label][key] = {
                    "summary": issue_info["summary"],
                    "change_date": change["date"],
                    "months": filtered_months,
                }

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

    group_items = "".join(
        f'<label class="ms-item"><input type="checkbox" value="{g}" onchange="buildPersonal()">{g}</label>'
        for g in group_names
    )

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
    <button onclick="expandAll('pBody')">Expandir todo</button>
    <button onclick="collapseAll('pBody')">Colapsar todo</button>
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

<div class="footer">Generado por jira-dashboard/report_hours.py</div>

<script>
const PERSONAL = {json.dumps(personal_data, ensure_ascii=False)};
const NEURO = {json.dumps(neuro_data, ensure_ascii=False)};
const CHANGES = {json.dumps(changes_data, ensure_ascii=False)};
const ALL_MONTHS = {json.dumps(months)};
const USER_GROUPS = {json.dumps(user_groups_map, ensure_ascii=False)};
const JIRA = "{jira_url}";
const MNAMES = {json.dumps(MONTH_NAMES)};
const collapsedYears = new Set();

function fmt(h) {{ return h === 0 ? '<span class="zero">-</span>' : h.toFixed(1); }}

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

function buildHeaders(theadId) {{
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
  let mr = '<tr><th>Nombre</th>';
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

function getSelectedGroups() {{
  const checks = document.querySelectorAll('#groupSelect input[type=checkbox]:checked');
  return Array.from(checks).map(c => c.value);
}}

function updateGroupLabel() {{
  const sel = getSelectedGroups();
  const lbl = document.getElementById('groupLabel');
  if (sel.length === 0) lbl.textContent = 'Todos los grupos';
  else if (sel.length <= 2) lbl.textContent = sel.join(', ');
  else lbl.textContent = sel.length + ' grupos';
}}

document.addEventListener('click', function(e) {{
  const ms = document.getElementById('groupSelect');
  if (ms && !ms.contains(e.target)) ms.classList.remove('open');
}});

function buildPersonal() {{
  updateGroupLabel();
  const vm = getVisibleMonths();
  const cols = getColumns(vm);
  const selGroups = getSelectedGroups();
  buildHeaders('pHead');

  const users = Object.keys(PERSONAL).sort();
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
      Object.values(tasks).forEach(t => {{
        vm.forEach(m => {{ const h = t.months[m] || 0; uM[m] += h; uTotal += h; }});
      }});
    }});
    if (uTotal === 0) return;

    let cells = '';
    cols.forEach((c, i) => {{ const v = colVal(uM, c); mTotals[i] += v; cells += '<td>' + fmt(v) + '</td>'; }});
    gt += uTotal;
    html += '<tr class="row-l0" data-id="' + uid + '">' +
      '<td onclick="toggle(\\'' + uid + '\\')">' +
      '<span class="arrow">&#9654;</span> ' + user + '</td>' + cells +
      '<td class="total">' + uTotal.toFixed(1) + '</td></tr>\\n';

    Object.keys(projs).sort().forEach(proj => {{
      const pid = 'p' + (rid++);
      const tasks = projs[proj];
      const pM = {{}};
      vm.forEach(m => pM[m] = 0);
      let pTotal = 0;
      Object.values(tasks).forEach(t => {{
        vm.forEach(m => {{ const h = t.months[m] || 0; pM[m] += h; pTotal += h; }});
      }});
      if (pTotal === 0) return;

      let pCells = '';
      cols.forEach(c => pCells += '<td>' + fmt(colVal(pM, c)) + '</td>');
      html += '<tr class="row-l1" data-id="' + pid + '" data-parent="' + uid + '" style="display:none">' +
        '<td onclick="toggle(\\'' + pid + '\\')">' +
        '<span class="arrow">&#9654;</span> ' + proj + '</td>' + pCells +
        '<td class="total">' + pTotal.toFixed(1) + '</td></tr>\\n';

      Object.keys(tasks).sort().forEach(issKey => {{
        const t = tasks[issKey];
        const tM = {{}};
        vm.forEach(m => tM[m] = t.months[m] || 0);
        let tT = 0;
        vm.forEach(m => tT += tM[m]);
        if (tT === 0) return;
        let tCells = '';
        cols.forEach(c => tCells += '<td>' + fmt(colVal(tM, c)) + '</td>');
        html += '<tr class="row-l2" data-parent="' + pid + '" style="display:none">' +
          '<td><a href="' + JIRA + '/browse/' + issKey + '" target="_blank">' + issKey + '</a> ' +
          t.summary.substring(0, 50) + '</td>' + tCells +
          '<td>' + tT.toFixed(1) + '</td></tr>\\n';
      }});
    }});
  }});

  let tCells = '';
  cols.forEach((c, i) => tCells += '<td class="total">' + mTotals[i].toFixed(1) + '</td>');
  html += '<tr class="row-totals"><td>TOTAL</td>' + tCells +
    '<td class="total grand">' + gt.toFixed(1) + '</td></tr>';
  document.getElementById('pBody').innerHTML = html;
}}

function buildNeuro() {{
  const vm = getVisibleMonths();
  const cols = getColumns(vm);
  buildHeaders('nHead');

  const parents = Object.keys(NEURO).sort();
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
      Object.values(tasks).forEach(t => {{
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

    Object.keys(children).sort().forEach(child => {{
      const cid = 'n' + (rid++);
      const tasks = children[child];
      const cM = {{}};
      vm.forEach(m => cM[m] = 0);
      let cTotal = 0;
      Object.values(tasks).forEach(t => {{
        vm.forEach(m => {{ const h = t.months[m] || 0; cM[m] += h; cTotal += h; }});
      }});
      if (cTotal === 0) return;

      let cCells = '';
      cols.forEach(c => cCells += '<td>' + fmt(colVal(cM, c)) + '</td>');
      html += '<tr class="row-l1" data-id="' + cid + '" data-parent="' + uid + '" style="display:none">' +
        '<td onclick="toggle(\\'' + cid + '\\')">' +
        '<span class="arrow">&#9654;</span> ' + child + '</td>' + cCells +
        '<td class="total">' + cTotal.toFixed(1) + '</td></tr>\\n';

      Object.keys(tasks).sort().forEach(issKey => {{
        const t = tasks[issKey];
        const tM = {{}};
        vm.forEach(m => tM[m] = t.months[m] || 0);
        let tT = 0;
        vm.forEach(m => tT += tM[m]);
        if (tT === 0) return;
        let tCells = '';
        cols.forEach(c => tCells += '<td>' + fmt(colVal(tM, c)) + '</td>');
        html += '<tr class="row-l2" data-parent="' + cid + '" style="display:none">' +
          '<td><a href="' + JIRA + '/browse/' + issKey + '" target="_blank">' + issKey + '</a> ' +
          t.summary.substring(0, 50) + '</td>' + tCells +
          '<td>' + tT.toFixed(1) + '</td></tr>\\n';
      }});
    }});
  }});

  let tCells = '';
  cols.forEach((c, i) => tCells += '<td class="total">' + mTotals[i].toFixed(1) + '</td>');
  html += '<tr class="row-totals"><td>TOTAL</td>' + tCells +
    '<td class="total grand">' + gt.toFixed(1) + '</td></tr>';
  document.getElementById('nBody').innerHTML = html;
}}

function buildChanges() {{
  const vm = getVisibleMonths();
  const cols = getColumns(vm);
  buildHeaders('chHead');

  const labels = Object.keys(CHANGES).sort();
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
    Object.values(tasks).forEach(t => {{
      vm.forEach(m => {{ const h = t.months[m] || 0; lM[m] += h; lTotal += h; }});
    }});
    if (lTotal === 0) return;

    let cells = '';
    cols.forEach((c, i) => {{ const v = colVal(lM, c); mTotals[i] += v; cells += '<td>' + fmt(v) + '</td>'; }});
    gt += lTotal;
    html += '<tr class="row-l0" data-id="' + lid + '">' +
      '<td onclick="toggle(\\'' + lid + '\\')">' +
      '<span class="arrow">&#9654;</span> ' + label + '</td>' + cells +
      '<td class="total">' + lTotal.toFixed(1) + '</td></tr>\\n';

    Object.keys(tasks).sort().forEach(issKey => {{
      const t = tasks[issKey];
      const tM = {{}};
      vm.forEach(m => tM[m] = t.months[m] || 0);
      let tT = 0;
      vm.forEach(m => tT += tM[m]);
      if (tT === 0) return;
      let tCells = '';
      cols.forEach(c => tCells += '<td>' + fmt(colVal(tM, c)) + '</td>');
      html += '<tr class="row-l1" data-parent="' + lid + '" style="display:none">' +
        '<td><a href="' + JIRA + '/browse/' + issKey + '" target="_blank">' + issKey + '</a> ' +
        t.summary.substring(0, 50) + ' <small style="color:#94a3b8">(' + t.change_date + ')</small></td>' + tCells +
        '<td class="total">' + tT.toFixed(1) + '</td></tr>\\n';
    }});
  }});

  let tCells = '';
  cols.forEach((c, i) => tCells += '<td class="total">' + mTotals[i].toFixed(1) + '</td>');
  html += '<tr class="row-totals"><td>TOTAL</td>' + tCells +
    '<td class="total grand">' + gt.toFixed(1) + '</td></tr>';
  document.getElementById('chBody').innerHTML = html;
}}

function switchTab(tab) {{
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.querySelector('.tab[data-tab="' + tab + '"]').classList.add('active');
  document.getElementById('tabPersonal').classList.toggle('active', tab === 'personal');
  document.getElementById('tabNeuro').classList.toggle('active', tab === 'neuro');
  document.getElementById('tabChanges').classList.toggle('active', tab === 'changes');
  if (tab === 'personal') buildPersonal();
  else if (tab === 'neuro') buildNeuro();
  else buildChanges();
}}

function expandAll(bodyId) {{
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
  updateSummary();
  const active = document.querySelector('.tab.active').dataset.tab;
  if (active === 'personal') buildPersonal();
  else if (active === 'neuro') buildNeuro();
  else buildChanges();
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

    months = build_months(args.date_from, args.date_to)
    raw = fetch_worklogs(client, args.date_from, args.date_to, allowed_ids)

    if not raw:
        print("Aviso: No se encontraron worklogs en el rango de fechas. Generando informe vacío.")

    # Fetch client field changes (uses JQL server-side filter)
    client_changes = fetch_client_changes(client, args.date_from, args.date_to)

    if args.format == "csv":
        out = args.output or f"output/horas_{args.date_from}_{args.date_to}.csv"
        path = generate_csv(raw, months, out)
    else:
        out = args.output or f"output/horas_{args.date_from}_{args.date_to}.html"
        path = generate_html(raw, months, groups_info, args.date_from, args.date_to,
                             config.jira_url, out, client_changes=client_changes)

    abs_path = os.path.abspath(path)
    print(f"\nInforme generado: {abs_path}")
    print(f"Abrir: file://{abs_path}")


if __name__ == "__main__":
    main()
