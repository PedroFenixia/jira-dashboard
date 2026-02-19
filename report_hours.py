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
        params = {"groupId": group_id, "maxResults": 200, "startAt": 0}
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
    jql = (
        f'worklogDate >= "{date_from}-01" AND worklogDate <= "{date_to}-31" '
        f"ORDER BY updated ASC"
    )
    fields = ["summary", "project", "worklog"]
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
            total_wl += 1

        if (idx + 1) % 50 == 0:
            print(f"  {idx + 1}/{len(issues)} issues procesadas...")

    print(f"  {total_wl} worklogs procesados")
    return raw


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


def generate_html(raw, months, groups_info, date_from, date_to, jira_url, output_path):
    """Generate interactive HTML with collapsible user → month → task rows."""
    users = sorted(raw.keys())

    # Prepare JSON data for JS
    js_data = {}
    for user in users:
        user_months = {}
        for m in months:
            tasks = raw[user].get(m, {})
            task_list = []
            for key, info in sorted(tasks.items()):
                task_list.append({
                    "key": key,
                    "summary": info["summary"],
                    "hours": round(info["hours"], 1),
                })
            user_months[m] = {
                "tasks": task_list,
                "total": round(sum(t["hours"] for t in task_list), 1),
            }
        js_data[user] = user_months

    # Compute totals for summary cards
    grand_total = 0
    for user in users:
        for m in months:
            grand_total += js_data[user][m]["total"]

    group_names = sorted(set(g for info in groups_info.values() for g in info["groups"])) if groups_info else []

    month_headers = ""
    for m in months:
        label = MONTH_NAMES.get(m[5:7], m[5:7])
        year_short = m[2:4]
        month_headers += f"<th>{label} {year_short}</th>"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Informe de Horas — {date_from} a {date_to}</title>
<style>
  :root {{
    --bg: #f8fafc; --card: #fff; --text: #1e293b; --muted: #64748b;
    --border: #e2e8f0; --blue: #3b82f6; --green: #22c55e; --amber: #f59e0b;
  }}
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    background: var(--bg); color: var(--text); padding: 24px;
    max-width: 1600px; margin: 0 auto;
  }}
  h1 {{ font-size: 1.5rem; margin-bottom: 4px; }}
  .meta {{ color: var(--muted); font-size: 0.85rem; margin-bottom: 8px; }}
  .groups {{ color: var(--muted); font-size: 0.8rem; margin-bottom: 20px; }}
  .groups span {{ background: #e0e7ff; color: #4338ca; padding: 2px 8px;
                  border-radius: 4px; font-size: 0.7rem; margin-right: 4px; }}
  .summary {{
    display: flex; gap: 16px; margin-bottom: 24px; flex-wrap: wrap;
  }}
  .stat {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px 24px; text-align: center;
  }}
  .stat .val {{ font-size: 1.75rem; font-weight: 700; color: var(--blue); }}
  .stat .lbl {{ font-size: 0.75rem; color: var(--muted); text-transform: uppercase; }}
  .controls {{
    display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; align-items: center;
  }}
  .controls button {{
    padding: 6px 14px; border: 1px solid var(--border); border-radius: 6px;
    background: var(--card); cursor: pointer; font-size: 0.8rem; color: var(--text);
  }}
  .controls button:hover {{ background: #f1f5f9; }}
  .controls button.active {{ background: var(--blue); color: white; border-color: var(--blue); }}
  .search {{
    padding: 6px 12px; border: 1px solid var(--border); border-radius: 6px;
    font-size: 0.8rem; width: 200px;
  }}
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

  /* User row */
  .row-user td {{ font-weight: 600; cursor: pointer; }}
  .row-user td:first-child {{ text-align: left; position: sticky; left: 0; background: var(--card); z-index: 1; }}
  .row-user:hover td {{ background: #f0f7ff; }}
  .row-user td .arrow {{ display: inline-block; width: 16px; transition: transform 0.2s; }}
  .row-user.open td .arrow {{ transform: rotate(90deg); }}
  .row-user td.total {{ color: var(--blue); }}

  /* Month sub-row */
  .row-month {{ display: none; }}
  .row-month td {{ font-size: 0.78rem; background: #fafbfc; cursor: pointer; }}
  .row-month td:first-child {{ text-align: left; padding-left: 32px; position: sticky; left: 0; background: #fafbfc; z-index: 1; }}
  .row-month:hover td {{ background: #f5f5f5; }}
  .row-month td .arrow {{ display: inline-block; width: 14px; transition: transform 0.2s; font-size: 0.7rem; }}
  .row-month.open td .arrow {{ transform: rotate(90deg); }}
  .row-month td.total {{ color: var(--amber); font-weight: 600; }}

  /* Task sub-row */
  .row-task {{ display: none; }}
  .row-task td {{ font-size: 0.75rem; background: #f8f9fa; color: var(--muted); }}
  .row-task td:first-child {{ text-align: left; padding-left: 52px; position: sticky; left: 0; background: #f8f9fa; z-index: 1; }}
  .row-task td a {{ color: var(--blue); text-decoration: none; }}
  .row-task td a:hover {{ text-decoration: underline; }}

  /* Totals row */
  .row-totals td {{ background: #f1f5f9; border-top: 2px solid var(--border); font-weight: 700; }}
  .row-totals td:first-child {{ text-align: left; position: sticky; left: 0; background: #f1f5f9; z-index: 1; }}
  .row-totals td.grand {{ color: var(--green); font-size: 0.9rem; }}

  .zero {{ color: #cbd5e1; }}
  .footer {{ text-align: center; color: var(--muted); font-size: 0.7rem; margin-top: 24px; }}
  @media print {{
    body {{ padding: 0; font-size: 0.65rem; }}
    .controls {{ display: none; }}
    .row-month, .row-task {{ display: none !important; }}
  }}
</style>
</head>
<body>
<h1>Informe de Horas Reportadas</h1>
<div class="meta">{date_from} — {date_to} &middot; Generado: {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>
<div class="groups">Grupos: {"".join(f'<span>{g}</span>' for g in group_names) if group_names else '<span>Todos</span>'}</div>

<div class="summary">
  <div class="stat"><div class="val">{len(users)}</div><div class="lbl">Usuarios</div></div>
  <div class="stat"><div class="val">{len(months)}</div><div class="lbl">Meses</div></div>
  <div class="stat"><div class="val">{grand_total:,.1f}</div><div class="lbl">Total horas</div></div>
  <div class="stat"><div class="val">{grand_total / len(months) if months else 0:,.1f}</div><div class="lbl">Media mensual</div></div>
</div>

<div class="controls">
  <button onclick="expandAll()">Expandir todo</button>
  <button onclick="collapseAll()">Colapsar todo</button>
  <input type="text" class="search" placeholder="Buscar usuario..." oninput="filterUsers(this.value)">
</div>

<div class="table-wrap">
<table>
<thead>
<tr><th>Nombre</th>{month_headers}<th>TOTAL</th></tr>
</thead>
<tbody id="tbody"></tbody>
</table>
</div>

<div class="footer">Generado por jira-dashboard/report_hours.py</div>

<script>
const DATA = {json.dumps(js_data, ensure_ascii=False)};
const MONTHS = {json.dumps(months)};
const JIRA = "{jira_url}";

const MNAMES = {json.dumps(MONTH_NAMES)};

function fmt(h) {{ return h === 0 ? '<span class="zero">-</span>' : h.toFixed(1); }}

function buildTable() {{
  const tbody = document.getElementById('tbody');
  let html = '';
  const users = Object.keys(DATA).sort();
  const monthTotals = {{}};
  MONTHS.forEach(m => monthTotals[m] = 0);
  let grandTotal = 0;

  users.forEach((user, ui) => {{
    const ud = DATA[user];
    let userTotal = 0;
    // User row
    let cells = '';
    MONTHS.forEach(m => {{
      const h = ud[m] ? ud[m].total : 0;
      userTotal += h;
      monthTotals[m] += h;
      cells += `<td>${{fmt(h)}}</td>`;
    }});
    grandTotal += userTotal;
    html += `<tr class="row-user" data-user="${{ui}}" onclick="toggleUser(${{ui}})">` +
      `<td><span class="arrow">&#9654;</span> ${{user}}</td>${{cells}}` +
      `<td class="total">${{userTotal.toFixed(1)}}</td></tr>\\n`;

    // Month sub-rows
    MONTHS.forEach((m, mi) => {{
      const md = ud[m];
      if (!md || md.total === 0) return;
      const label = MNAMES[m.slice(5)] + ' ' + m.slice(2,4);
      const tasks = md.tasks || [];
      // Month row: only shows total in the corresponding month column
      let mCells = '';
      MONTHS.forEach((m2, mi2) => {{
        mCells += mi2 === mi ? `<td class="total">${{md.total.toFixed(1)}}</td>` : '<td></td>';
      }});
      html += `<tr class="row-month" data-user="${{ui}}" data-month="${{ui}}-${{mi}}" onclick="toggleMonth(${{ui}},${{mi}})">` +
        `<td><span class="arrow">&#9654;</span> ${{label}} (${{tasks.length}} tareas)</td>${{mCells}}` +
        `<td class="total">${{md.total.toFixed(1)}}</td></tr>\\n`;

      // Task sub-rows
      tasks.forEach(t => {{
        let tCells = '';
        MONTHS.forEach((m2, mi2) => {{
          tCells += mi2 === mi ? `<td>${{t.hours.toFixed(1)}}</td>` : '<td></td>';
        }});
        html += `<tr class="row-task" data-month="${{ui}}-${{mi}}">` +
          `<td><a href="${{JIRA}}/browse/${{t.key}}" target="_blank">${{t.key}}</a> ${{t.summary.substring(0,60)}}</td>${{tCells}}` +
          `<td>${{t.hours.toFixed(1)}}</td></tr>\\n`;
      }});
    }});
  }});

  // Totals row
  let tCells = '';
  MONTHS.forEach(m => tCells += `<td class="total">${{monthTotals[m].toFixed(1)}}</td>`);
  html += `<tr class="row-totals"><td>TOTAL</td>${{tCells}}<td class="total grand">${{grandTotal.toFixed(1)}}</td></tr>`;

  tbody.innerHTML = html;
}}

function toggleUser(ui) {{
  const row = document.querySelector(`.row-user[data-user="${{ui}}"]`);
  row.classList.toggle('open');
  const show = row.classList.contains('open');
  document.querySelectorAll(`.row-month[data-user="${{ui}}"]`).forEach(r => {{
    r.style.display = show ? '' : 'none';
    if (!show) {{
      r.classList.remove('open');
      // also hide tasks
      const mid = r.dataset.month;
      document.querySelectorAll(`.row-task[data-month="${{mid}}"]`).forEach(t => t.style.display = 'none');
    }}
  }});
}}

function toggleMonth(ui, mi) {{
  event.stopPropagation();
  const mid = `${{ui}}-${{mi}}`;
  const row = document.querySelector(`.row-month[data-month="${{mid}}"]`);
  row.classList.toggle('open');
  const show = row.classList.contains('open');
  document.querySelectorAll(`.row-task[data-month="${{mid}}"]`).forEach(r => {{
    r.style.display = show ? '' : 'none';
  }});
}}

function expandAll() {{
  document.querySelectorAll('.row-user').forEach(r => {{
    r.classList.add('open');
  }});
  document.querySelectorAll('.row-month').forEach(r => {{
    r.style.display = '';
    r.classList.add('open');
  }});
  document.querySelectorAll('.row-task').forEach(r => r.style.display = '');
}}

function collapseAll() {{
  document.querySelectorAll('.row-user').forEach(r => r.classList.remove('open'));
  document.querySelectorAll('.row-month').forEach(r => {{
    r.style.display = 'none';
    r.classList.remove('open');
  }});
  document.querySelectorAll('.row-task').forEach(r => r.style.display = 'none');
}}

function filterUsers(q) {{
  q = q.toLowerCase();
  document.querySelectorAll('.row-user').forEach(r => {{
    const name = r.querySelector('td').textContent.toLowerCase();
    const show = name.includes(q);
    r.style.display = show ? '' : 'none';
    const ui = r.dataset.user;
    if (!show) {{
      document.querySelectorAll(`.row-month[data-user="${{ui}}"]`).forEach(m => m.style.display = 'none');
      document.querySelectorAll('.row-task').forEach(t => {{
        if (t.dataset.month.startsWith(ui + '-')) t.style.display = 'none';
      }});
    }}
  }});
}}

buildTable();
</script>
</body>
</html>"""

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    return output_path


def main():
    parser = argparse.ArgumentParser(description="Informe de horas JIRA por usuario y mes")
    parser.add_argument("--from", dest="date_from", default="2024-01",
                        help="Mes inicio YYYY-MM (default: 2024-01)")
    parser.add_argument("--to", dest="date_to", default="2026-01",
                        help="Mes fin YYYY-MM (default: 2026-01)")
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
        print("No se encontraron worklogs en el rango de fechas.")
        return

    if args.format == "csv":
        out = args.output or f"output/horas_{args.date_from}_{args.date_to}.csv"
        path = generate_csv(raw, months, out)
    else:
        out = args.output or f"output/horas_{args.date_from}_{args.date_to}.html"
        path = generate_html(raw, months, groups_info, args.date_from, args.date_to,
                             config.jira_url, out)

    abs_path = os.path.abspath(path)
    print(f"\nInforme generado: {abs_path}")
    print(f"Abrir: file://{abs_path}")


if __name__ == "__main__":
    main()
