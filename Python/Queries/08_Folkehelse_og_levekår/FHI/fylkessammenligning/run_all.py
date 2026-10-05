"""
Run the full fylkessammenligning analysis end-to-end:
1. Run every generated fetch script under scripts/ (writes CSVs to data/).
2. Run aggregate_comparison.py to build the ranked summary.
3. Run build_dashboard.py to render the dashboard HTML.

Usage:
    python run_all.py
"""

import os
import subprocess
import sys
from pathlib import Path

pythonpath = os.environ.get("PYTHONPATH")
if not pythonpath:
    current = Path(__file__).resolve()
    while current.name != "Python" and current != current.parent:
        current = current.parent
    pythonpath = str(current)
    os.environ["PYTHONPATH"] = pythonpath

BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR / "scripts"


def find_scripts():
    return sorted(SCRIPTS_DIR.rglob("*.py"))


def run_script(script_path):
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [sys.executable, str(script_path)],
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.returncode, result.stdout, result.stderr


def main():
    scripts = find_scripts()
    print(f"Found {len(scripts)} fetch scripts\n")

    failures = []
    for i, script_path in enumerate(scripts, start=1):
        name = script_path.stem
        print(f"[{i}/{len(scripts)}] {name} ...", end=" ")
        returncode, stdout, stderr = run_script(script_path)
        if returncode == 0:
            print("OK")
        else:
            print("FAILED")
            failures.append((name, stderr.strip().splitlines()[-1] if stderr.strip() else "unknown error"))

    print(f"\n{len(scripts) - len(failures)}/{len(scripts)} scripts succeeded")
    if failures:
        print("\nFailures:")
        for name, err in failures:
            print(f"  - {name}: {err}")

    print("\nRunning aggregation...")
    subprocess.run([sys.executable, str(BASE_DIR / "aggregate_comparison.py")], env={**os.environ, "PYTHONIOENCODING": "utf-8"})

    print("\nBuilding dashboard...")
    subprocess.run([sys.executable, str(BASE_DIR / "build_dashboard.py")], env={**os.environ, "PYTHONIOENCODING": "utf-8"})


if __name__ == "__main__":
    main()
