import os
import sys
from dotenv import load_dotenv


class Config:
    def __init__(self):
        load_dotenv()
        self.jira_url = os.getenv("JIRA_URL", "").rstrip("/")
        self.jira_email = os.getenv("JIRA_EMAIL", "")
        self.jira_api_token = os.getenv("JIRA_API_TOKEN", "")
        self.project_key = os.getenv("JIRA_PROJECT_KEY", "ALL")
        self.all_projects = self.project_key.upper() == "ALL"
        self.story_points_field = os.getenv("STORY_POINTS_FIELD", "")
        self.sprint_count = int(os.getenv("SPRINT_COUNT") or "6")
        self.max_boards = int(os.getenv("MAX_BOARDS") or "20")
        self._validate()

    def _validate(self):
        missing = []
        for var, attr in [
            ("JIRA_URL", "jira_url"),
            ("JIRA_EMAIL", "jira_email"),
            ("JIRA_API_TOKEN", "jira_api_token"),
        ]:
            if not getattr(self, attr):
                missing.append(var)
        if missing:
            print(f"Error: Faltan variables de entorno: {', '.join(missing)}")
            print("Copia .env.example a .env y rellena los valores.")
            sys.exit(1)

    @property
    def auth(self):
        return (self.jira_email, self.jira_api_token)

    @property
    def agile_url(self):
        return f"{self.jira_url}/rest/agile/1.0"

    @property
    def api_url(self):
        return f"{self.jira_url}/rest/api/3"
