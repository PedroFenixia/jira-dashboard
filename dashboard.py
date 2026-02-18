import os
from jinja2 import Environment, FileSystemLoader


class DashboardGenerator:
    def __init__(self):
        template_dir = os.path.join(os.path.dirname(__file__), "templates")
        self.env = Environment(loader=FileSystemLoader(template_dir))

    def generate(self, metrics, output_path):
        template = self.env.get_template("dashboard.html")
        html = template.render(metrics=metrics)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        return output_path
