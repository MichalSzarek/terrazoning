from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from app.services.operations_scope import provinces


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TerraZoning province campaign rollout")
    parser.add_argument(
        "--province",
        action="append",
        dest="provinces",
        choices=provinces(),
        help="Province to run. Defaults to both rollout provinces.",
    )
    parser.add_argument("--autofix", action="store_true", help="Enable conservative self-heal actions")
    parser.add_argument("--parallel", action="store_true", help="Enable parallel report gathering")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    selected_provinces = args.provinces or provinces()
    backend_dir = Path(__file__).resolve().parent

    for province in selected_provinces:
        command = [
            sys.executable,
            "run_province_campaign.py",
            "--province",
            province,
            "--stage",
            "full",
        ]
        if args.autofix:
            command.append("--autofix")
        if args.parallel:
            command.append("--parallel")
        subprocess.run(command, cwd=backend_dir, check=True)


if __name__ == "__main__":
    main()
