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
        config.story_points_field = client.detect_story_points_field()
        print(f"  Campo: {config.story_points_field}")

    # 4. Find board
    print(f"Buscando board para proyecto {config.project_key}...")
    boards = client.get_boards()
    if not boards:
        print(f"Error: No se encontró board para {config.project_key}")
        sys.exit(1)
    board = boards[0]
    board_id = board["id"]
    print(f"  Board: {board.get('name', board_id)} (ID: {board_id})")

    # 5. Fetch sprints
    print("Obteniendo sprints...")
    sprints = client.get_sprints(board_id)
    closed = [s for s in sprints if s.get("state") == "closed"]
    active = [s for s in sprints if s.get("state") == "active"]
    recent = closed[-config.sprint_count:]
    print(f"  {len(closed)} cerrados, {len(active)} activos — analizando últimos {len(recent)}")

    # 6. Fetch sprint issues
    print("Obteniendo issues por sprint...")
    sprints_data = []
    for sprint in recent + active:
        issues = client.get_sprint_issues(board_id, sprint["id"])
        sprints_data.append({"sprint": sprint, "issues": issues})
        print(f"  {sprint['name']}: {len(issues)} issues")

    # 7. Fetch all project issues
    print("Obteniendo todas las issues del proyecto...")
    project_issues = client.get_project_issues()
    print(f"  Total: {len(project_issues)} issues")

    # 8. Fetch changelogs for cycle time
    print("Obteniendo changelogs para cycle time...")
    resolved = [i for i in project_issues if i.get("fields", {}).get("resolutiondate")]
    sample = resolved[-100:]  # Limitar a 100 para no saturar la API
    changelogs = {}
    for idx, issue in enumerate(sample):
        key = issue["key"]
        changelogs[key] = client.get_issue_changelog(key)
        if (idx + 1) % 25 == 0:
            print(f"  {idx + 1}/{len(sample)} changelogs procesados")
    print(f"  {len(changelogs)} changelogs obtenidos")

    # 9. Process metrics
    print("Calculando métricas...")
    processor = DataProcessor(config)
    metrics = processor.process_all(sprints_data, project_issues, changelogs)

    # 10. Generate dashboard
    print("Generando dashboard...")
    generator = DashboardGenerator()
    output_path = generator.generate(metrics, args.output)
    abs_path = os.path.abspath(output_path)
    print(f"\nDashboard generado: {abs_path}")
    print(f"Abrir en navegador: file://{abs_path}")


if __name__ == "__main__":
    main()
