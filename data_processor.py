from datetime import datetime, timedelta
from collections import defaultdict
import statistics


def _parse_date(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _week_label(dt):
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


class DataProcessor:
    def __init__(self, config):
        self.sp_field = config.story_points_field or "customfield_10016"
        self.project_key = "Todos los proyectos" if config.all_projects else config.project_key

    def _get_sp(self, issue):
        return issue.get("fields", {}).get(self.sp_field) or 0

    def _status_category(self, issue):
        return (issue.get("fields", {})
                .get("status", {})
                .get("statusCategory", {})
                .get("key", ""))

    # ── Public API ───────────────────────────────────────────────

    def process_all(self, sprints_data, project_issues, changelogs,
                     worklogs=None, jira_url=""):
        # Build issue key → summary map for hours report links
        issue_map = {}
        for i in project_issues:
            key = i.get("key", "")
            summary = i.get("fields", {}).get("summary", "")
            if key:
                issue_map[key] = summary

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "project_key": self.project_key,
            "jira_url": jira_url,
            "sprint_metrics": self._sprint_metrics(sprints_data),
            "velocity": self._velocity(sprints_data),
            "status_distribution": self._status_distribution(project_issues),
            "issue_types": self._issue_type_distribution(project_issues),
            "assignee_workload": self._assignee_workload(project_issues),
            "created_vs_resolved": self._created_vs_resolved(project_issues),
            "backlog": self._backlog(project_issues),
            "time_tracking": self._time_tracking(project_issues),
            "cycle_time": self._cycle_time(changelogs),
            "lead_time": self._lead_time(project_issues),
            "priority_distribution": self._priority_distribution(project_issues),
            "hours_report": self._hours_report(worklogs or [], issue_map),
        }

    # ── Sprint metrics ───────────────────────────────────────────

    def _sprint_metrics(self, sprints_data):
        metrics = []
        for entry in sprints_data:
            sprint = entry["sprint"]
            issues = entry["issues"]
            total = len(issues)
            done = [i for i in issues if self._status_category(i) == "done"]
            committed_sp = sum(self._get_sp(i) for i in issues)
            completed_sp = sum(self._get_sp(i) for i in done)
            rate = round(len(done) / total * 100, 1) if total else 0
            metrics.append({
                "name": sprint.get("name", ""),
                "state": sprint.get("state", ""),
                "total_issues": total,
                "completed_issues": len(done),
                "completion_rate": rate,
                "committed_sp": committed_sp,
                "completed_sp": completed_sp,
            })
        return metrics

    def _velocity(self, sprints_data):
        sm = self._sprint_metrics(sprints_data)
        closed = [s for s in sm if s["state"] == "closed"]
        labels = [s["name"] for s in closed]
        committed = [s["committed_sp"] for s in closed]
        completed = [s["completed_sp"] for s in closed]
        avg = round(statistics.mean(completed), 1) if completed else 0
        return {
            "labels": labels,
            "committed": committed,
            "completed": completed,
            "avg_velocity": avg,
        }

    # ── Issue distributions ──────────────────────────────────────

    def _status_distribution(self, issues):
        counts = defaultdict(int)
        for i in issues:
            name = i.get("fields", {}).get("status", {}).get("name", "Unknown")
            counts[name] += 1
        sorted_items = sorted(counts.items(), key=lambda x: -x[1])
        return {
            "labels": [x[0] for x in sorted_items],
            "counts": [x[1] for x in sorted_items],
        }

    def _issue_type_distribution(self, issues):
        counts = defaultdict(int)
        for i in issues:
            name = i.get("fields", {}).get("issuetype", {}).get("name", "Other")
            counts[name] += 1
        sorted_items = sorted(counts.items(), key=lambda x: -x[1])
        return {
            "labels": [x[0] for x in sorted_items],
            "counts": [x[1] for x in sorted_items],
        }

    def _priority_distribution(self, issues):
        counts = defaultdict(int)
        for i in issues:
            name = i.get("fields", {}).get("priority", {}).get("name", "None")
            counts[name] += 1
        order = ["Highest", "High", "Medium", "Low", "Lowest"]
        labels = [p for p in order if p in counts] + \
                 [p for p in counts if p not in order]
        return {
            "labels": labels,
            "counts": [counts[l] for l in labels],
        }

    def _assignee_workload(self, issues):
        open_issues = [i for i in issues if self._status_category(i) != "done"]
        counts = defaultdict(int)
        for i in open_issues:
            assignee = i.get("fields", {}).get("assignee")
            name = assignee.get("displayName", "Sin asignar") if assignee else "Sin asignar"
            counts[name] += 1
        sorted_items = sorted(counts.items(), key=lambda x: -x[1])
        return {
            "labels": [x[0] for x in sorted_items],
            "counts": [x[1] for x in sorted_items],
        }

    # ── Flow metrics ─────────────────────────────────────────────

    def _created_vs_resolved(self, issues):
        now = datetime.now().astimezone()
        weeks = 12
        start = now - timedelta(weeks=weeks)
        created_by_week = defaultdict(int)
        resolved_by_week = defaultdict(int)

        for i in issues:
            fields = i.get("fields", {})
            created = _parse_date(fields.get("created"))
            resolved = _parse_date(fields.get("resolutiondate"))
            if created and created >= start:
                created_by_week[_week_label(created)] += 1
            if resolved and resolved >= start:
                resolved_by_week[_week_label(resolved)] += 1

        labels = []
        d = start
        while d <= now:
            labels.append(_week_label(d))
            d += timedelta(weeks=1)

        return {
            "labels": labels,
            "created": [created_by_week.get(w, 0) for w in labels],
            "resolved": [resolved_by_week.get(w, 0) for w in labels],
        }

    def _backlog(self, issues):
        open_issues = [i for i in issues if self._status_category(i) != "done"]
        by_type = defaultdict(int)
        by_priority = defaultdict(int)
        for i in open_issues:
            fields = i.get("fields", {})
            by_type[fields.get("issuetype", {}).get("name", "Other")] += 1
            by_priority[fields.get("priority", {}).get("name", "None")] += 1
        return {
            "total": len(open_issues),
            "by_type": dict(by_type),
            "by_priority": dict(by_priority),
        }

    # ── Time metrics ─────────────────────────────────────────────

    def _time_tracking(self, issues):
        labels, estimated, actual = [], [], []
        for i in issues:
            tt = i.get("fields", {}).get("timetracking", {})
            est = tt.get("originalEstimateSeconds")
            spent = tt.get("timeSpentSeconds")
            if est and spent:
                labels.append(i["key"])
                estimated.append(round(est / 3600, 1))
                actual.append(round(spent / 3600, 1))

        # Keep top 20 by variance
        if len(labels) > 20:
            combined = sorted(
                zip(labels, estimated, actual),
                key=lambda x: abs(x[1] - x[2]),
                reverse=True,
            )[:20]
            labels = [c[0] for c in combined]
            estimated = [c[1] for c in combined]
            actual = [c[2] for c in combined]

        avg_ratio = (round(sum(actual) / sum(estimated), 2)
                     if estimated and sum(estimated) > 0 else 0)

        return {
            "labels": labels,
            "estimated": estimated,
            "actual": actual,
            "accuracy_ratio": avg_ratio,
        }

    def _cycle_time(self, changelogs):
        """Cycle time = first 'In Progress' transition → Done."""
        cycle_times = []
        by_type = defaultdict(list)

        for issue_key, histories in changelogs.items():
            in_progress_date = None
            done_date = None
            for history in histories:
                for item in history.get("items", []):
                    if item.get("field") != "status":
                        continue
                    to_cat = item.get("to", "")
                    # statusCategory IDs: 2=new, 4=indeterminate, 3=done
                    # But in changelog we get the status name, check toString
                    to_name = (item.get("toString") or "").lower()
                    if not in_progress_date and to_name in (
                        "in progress", "en progreso", "in development",
                        "en desarrollo", "in review", "en revisión",
                    ):
                        in_progress_date = _parse_date(history.get("created"))
                    if to_name in ("done", "hecho", "closed", "cerrado",
                                   "resolved", "resuelto"):
                        done_date = _parse_date(history.get("created"))

            if in_progress_date and done_date and done_date > in_progress_date:
                days = (done_date - in_progress_date).total_seconds() / 86400
                cycle_times.append(days)

        if not cycle_times:
            return {"avg_days": 0, "median_days": 0, "distribution": [],
                    "dist_labels": []}

        avg = round(statistics.mean(cycle_times), 1)
        median = round(statistics.median(cycle_times), 1)

        # Histogram buckets
        buckets = [(0, 2), (2, 5), (5, 10), (10, 20), (20, float("inf"))]
        bucket_labels = ["0-2d", "2-5d", "5-10d", "10-20d", "20d+"]
        distribution = [0] * len(buckets)
        for ct in cycle_times:
            for idx, (lo, hi) in enumerate(buckets):
                if lo <= ct < hi:
                    distribution[idx] += 1
                    break

        return {
            "avg_days": avg,
            "median_days": median,
            "distribution": distribution,
            "dist_labels": bucket_labels,
        }

    def _lead_time(self, issues):
        """Lead time = created → resolutiondate."""
        by_type = defaultdict(list)

        for i in issues:
            fields = i.get("fields", {})
            created = _parse_date(fields.get("created"))
            resolved = _parse_date(fields.get("resolutiondate"))
            if created and resolved:
                days = (resolved - created).total_seconds() / 86400
                itype = fields.get("issuetype", {}).get("name", "Other")
                by_type[itype].append(days)

        all_times = [d for times in by_type.values() for d in times]
        avg = round(statistics.mean(all_times), 1) if all_times else 0
        median = round(statistics.median(all_times), 1) if all_times else 0

        labels = sorted(by_type.keys())
        averages = [round(statistics.mean(by_type[t]), 1) for t in labels]

        return {
            "avg_days": avg,
            "median_days": median,
            "labels": labels,
            "averages": averages,
        }

    # ── Hours report (worklogs) ──────────────────────────────────

    def _hours_report(self, worklogs, issue_map=None):
        """Hours logged per user per month, hierarchical: user → project → issue."""
        issue_map = issue_map or {}

        # Collect: {user: {project: {issue_key: {month: hours}}}}
        tree = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(float))))
        all_months = set()

        for wl in worklogs:
            author = wl.get("author", {})
            name = author.get("displayName", "Desconocido")
            seconds = wl.get("timeSpentSeconds", 0)
            started = _parse_date(wl.get("started"))
            issue_key = wl.get("_issueKey", "???")
            if not started or not seconds:
                continue
            month = started.strftime("%Y-%m")
            hours = seconds / 3600
            project = issue_key.split("-")[0] if "-" in issue_key else "OTHER"
            tree[name][project][issue_key][month] += hours
            all_months.add(month)

        months = sorted(all_months)

        def _sum_months(month_dict):
            return {m: round(month_dict.get(m, 0), 1) for m in months}

        def _total(month_dict):
            return round(sum(month_dict.get(m, 0) for m in months), 1)

        rows = []
        for user in sorted(tree.keys()):
            user_months = defaultdict(float)
            projects = []
            for proj_key in sorted(tree[user].keys()):
                proj_months = defaultdict(float)
                issues = []
                for iss_key in sorted(tree[user][proj_key].keys()):
                    iss_m = tree[user][proj_key][iss_key]
                    for m, h in iss_m.items():
                        proj_months[m] += h
                        user_months[m] += h
                    summary = issue_map.get(iss_key, "")
                    issues.append({
                        "key": iss_key,
                        "summary": summary,
                        "months": _sum_months(iss_m),
                        "total": _total(iss_m),
                    })
                issues.sort(key=lambda x: -x["total"])
                projects.append({
                    "key": proj_key,
                    "months": _sum_months(proj_months),
                    "total": _total(proj_months),
                    "issues": issues,
                })
            projects.sort(key=lambda x: -x["total"])
            rows.append({
                "user": user,
                "months": _sum_months(user_months),
                "total": _total(user_months),
                "projects": projects,
            })

        rows.sort(key=lambda r: -r["total"])

        month_totals = {}
        for m in months:
            month_totals[m] = round(sum(r["months"].get(m, 0) for r in rows), 1)
        grand_total = round(sum(r["total"] for r in rows), 1)

        return {
            "months": months,
            "rows": rows,
            "month_totals": month_totals,
            "grand_total": grand_total,
        }
