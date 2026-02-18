#!/usr/bin/env python3
"""JIRA Cloud Dashboard Generator.

Usage: python main.py [--output path/to/output.html]
"""
import argparse
import os
import sys

from config import Config
from jira_client import JiraClient
from data_processor import DataProcessor
from dashboard import DashboardGenerator


def main():
    parser = argparse.ArgumentParser(description="Genera dashboard JIRA")
    parser.add_argument(
        "-o", "--output",
        default="output/dashboard.html",
        help="Ruta del archivo HTML de salida (default: output/dashboard.html)",
    )
    args = parser.parse_args()

    # 1. Config
    print("Cargando configuración...")
    config = Config()

    # 2. Client
    print(f"Conectando a {config.jira_url}...")
    client = JiraClient(config)

    # 3. Auto-detect story points field
    if not config.story_points_field:
        print("Detectando campo de Story Points...")
        try:
            config.story_points_field = client.detect_story_points_field()
            print(f"  Campo: {config.story_points_field}")
        except Exception:
            config.story_points_field = "customfield_10016"
            print(f"  Usando default: {config.story_points_field}")

    # 4. Find boards
    scope = "todos los proyectos" if config.all_projects else config.project_key
    print(f"Buscando boards ({scope})...")
    boards = client.get_boards()
    if not boards:
        print("Aviso: No se encontraron boards. Se generará dashboard solo con issues.")
    else:
        print(f"  {len(boards)} boards encontrados")

    # 5. Fetch sprints from all boards
    print("Obteniendo sprints...")
    sprints_data = []
    for board in boards:
        board_id = board["id"]
        board_name = board.get("name", str(board_id))
        sprints = client.get_sprints(board_id)
        if not sprints:
            continue
        closed = [s for s in sprints if s.get("state") == "closed"]
        active = [s for s in sprints if s.get("state") == "active"]
        recent = closed[-config.sprint_count:]
        for sprint in recent + active:
            try:
                issues = client.get_sprint_issues(board_id, sprint["id"])
                sprints_data.append({"sprint": sprint, "issues": issues})
                print(f"  [{board_name}] {sprint['name']}: {len(issues)} issues")
            except Exception as e:
                print(f"  [{board_name}] {sprint['name']}: error, saltando ({e})")

    print(f"  Total: {len(sprints_data)} sprints analizados")

    # 6. Fetch all project issues
    print(f"Obteniendo issues ({scope}, últimos 6 meses)...")
    try:
        project_issues = client.get_project_issues()
    except Exception as e:
        print(f"  Error obteniendo issues: {e}")
        project_issues = []
    print(f"  Total: {len(project_issues)} issues")

    # 7. Fetch changelogs for cycle time (limit 50 for multi-project)
    max_changelogs = 50 if config.all_projects else 100
    print(f"Obteniendo changelogs para cycle time (máx {max_changelogs})...")
    resolved = [i for i in project_issues if i.get("fields", {}).get("resolutiondate")]
    sample = resolved[-max_changelogs:]
    changelogs = {}
    for idx, issue in enumerate(sample):
        key = issue["key"]
        try:
            changelogs[key] = client.get_issue_changelog(key)
        except Exception:
            pass
        if (idx + 1) % 25 == 0:
            print(f"  {idx + 1}/{len(sample)} changelogs procesados")
    print(f"  {len(changelogs)} changelogs obtenidos")

    # 8. Process metrics
    print("Calculando métricas...")
    processor = DataProcessor(config)
    metrics = processor.process_all(sprints_data, project_issues, changelogs)

    # 9. Generate dashboard
    print("Generando dashboard...")
    generator = DashboardGenerator()
    output_path = generator.generate(metrics, args.output)
    abs_path = os.path.abspath(output_path)
    print(f"\nDashboard generado: {abs_path}")
    print(f"Abrir en navegador: file://{abs_path}")


if __name__ == "__main__":
    main()
