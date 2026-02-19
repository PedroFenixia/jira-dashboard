import sys
import time
import requests


class JiraClient:
    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.session.auth = config.auth
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
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
                    print(f"  Rate limit. Esperando {wait}s...")
                    time.sleep(wait)
                    continue

                if resp.status_code == 401:
                    print("Error: Autenticación fallida. Revisa JIRA_EMAIL y JIRA_API_TOKEN.")
                    sys.exit(1)

                if resp.status_code == 403:
                    print(f"Error: Permiso denegado para {url}")
                    sys.exit(1)

                if resp.status_code >= 500:
                    wait = 2 ** attempt
                    print(f"  Error servidor ({resp.status_code}). Reintento en {wait}s...")
                    time.sleep(wait)
                    continue

                print(f"Error {resp.status_code}: {resp.text[:300]}")
                raise RuntimeError(f"HTTP {resp.status_code}")

            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError):
                wait = 2 ** attempt
                print(f"  Error de conexión. Reintento en {wait}s...")
                time.sleep(wait)

        print(f"Fallo tras {max_retries} reintentos: {url}")
        raise RuntimeError(f"Failed after {max_retries} retries: {url}")

    # ── Pagination ───────────────────────────────────────────────

    def _paginate_offset(self, url, params=None, key="values"):
        """Offset pagination (agile API): startAt / maxResults."""
        params = dict(params or {})
        params.setdefault("maxResults", 50)
        params["startAt"] = 0
        results = []

        while True:
            data = self._request("GET", url, params=params).json()
            page = data.get(key, [])
            results.extend(page)
            if data.get("isLast", True) or len(page) < params["maxResults"]:
                break
            params["startAt"] += params["maxResults"]

        return results

    def _search_issues(self, jql, fields, expand=None):
        """Token pagination using POST /rest/api/3/search/jql."""
        url = f"{self.config.api_url}/search/jql"
        all_issues = []
        next_token = None

        while True:
            body = {"jql": jql, "fields": fields, "maxResults": 100}
            if next_token:
                body["nextPageToken"] = next_token
            if expand:
                body["expand"] = expand

            resp = self._request("POST", url, json=body)
            data = resp.json()
            all_issues.extend(data.get("issues", []))
            next_token = data.get("nextPageToken")
            if not next_token:
                break

        return all_issues

    # ── Board & Sprint endpoints ─────────────────────────────────

    def get_boards(self):
        url = f"{self.config.agile_url}/board"
        params = {}
        if not self.config.all_projects:
            params["projectKeyOrId"] = self.config.project_key
        boards = self._paginate_offset(url, params=params)
        if self.config.all_projects:
            boards = boards[:self.config.max_boards]
        return boards

    def get_sprints(self, board_id):
        url = f"{self.config.agile_url}/board/{board_id}/sprint"
        try:
            return self._paginate_offset(url)
        except (RuntimeError, Exception):
            # Some boards don't support sprints (Kanban without sprints)
            print(f"    Board {board_id}: sin sprints, saltando...")
            return []

    def get_sprint_issues(self, board_id, sprint_id):
        url = f"{self.config.agile_url}/board/{board_id}/sprint/{sprint_id}/issue"
        return self._paginate_offset(url, key="issues")

    # ── Issue queries ────────────────────────────────────────────

    def get_project_issues(self):
        sp_field = self.config.story_points_field or "customfield_10016"
        fields = [
            "summary", "status", "assignee", "issuetype", "priority",
            "created", "updated", "resolutiondate", "resolution",
            "timetracking", "labels", "components", sp_field,
        ]
        if self.config.all_projects:
            # All projects, last 6 months to keep it manageable
            jql = "created >= -180d ORDER BY created ASC"
        else:
            jql = f"project = {self.config.project_key} ORDER BY created ASC"
        return self._search_issues(jql, fields)

    def get_issue_changelog(self, issue_key):
        url = f"{self.config.api_url}/issue/{issue_key}"
        data = self._request("GET", url, params={"expand": "changelog"}).json()
        return data.get("changelog", {}).get("histories", [])

    def get_issue_worklogs(self, issue_key):
        url = f"{self.config.api_url}/issue/{issue_key}/worklog"
        return self._paginate_offset(url, key="worklogs")

    # ── Field discovery ──────────────────────────────────────────

    def detect_story_points_field(self):
        url = f"{self.config.api_url}/field"
        data = self._request("GET", url).json()
        for field in data:
            name = (field.get("name") or "").lower()
            if "story point" in name:
                return field["id"]
        return "customfield_10016"
