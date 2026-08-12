from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".reference-generation"
EXPECTED_PATHS = {
    "dags/shared_gpu_release_rehearsal.py",
    "tools/run_rehearsal.py",
    "reports/dag_contract.csv",
    "reports/pool_configuration.csv",
    "reports/run_outcomes.csv",
    "reports/task_instance_evidence.csv",
    "reports/lease_events.csv",
}
FIXED_TIME = (2026, 8, 12, 0, 0, 0)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    if WORK.exists():
        shutil.rmtree(WORK)
    WORK.mkdir()
    input_root = WORK / "input"
    with zipfile.ZipFile(ROOT / "task" / "输入数据包.zip") as archive:
        archive.extractall(input_root)

    output_root = WORK / "output"
    airflow_home = WORK / "airflow-home"
    env = dict(os.environ)
    env.update(
        {
            "AIRFLOW_HOME": str(airflow_home),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__FERNET_KEY": "",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{airflow_home / 'airflow.db'}",
            "ALE_NETWORK_GUARD": "1",
            "PYTHONPATH": str(ROOT / "qa" / "network_guard"),
        }
    )
    migrate = subprocess.run(["airflow", "db", "migrate"], text=True, capture_output=True, env=env, check=False)
    if migrate.returncode != 0:
        raise RuntimeError(migrate.stdout + migrate.stderr)
    process = subprocess.run(
        [
            sys.executable,
            str(ROOT / "implementation" / "tools" / "run_rehearsal.py"),
            "--input",
            str(input_root / "input_data"),
            "--output",
            str(output_root),
            "--dag-file",
            str(ROOT / "implementation" / "dags" / "shared_gpu_release_rehearsal.py"),
        ],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    if process.returncode != 0:
        raise RuntimeError(process.stdout + process.stderr)

    paths = {path.relative_to(output_root).as_posix() for path in output_root.rglob("*") if path.is_file()}
    directories = {path.relative_to(output_root).as_posix() for path in output_root.rglob("*") if path.is_dir()}
    if paths != EXPECTED_PATHS:
        raise AssertionError(f"unexpected reference paths: {sorted(paths ^ EXPECTED_PATHS)}")
    if directories != {"dags", "tools", "reports"}:
        raise AssertionError(f"unexpected reference directories: {sorted(directories)}")

    package = ROOT / "reference-candidate.zip"
    with zipfile.ZipFile(package, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(output_root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(output_root).as_posix()
            info = zipfile.ZipInfo(relative, FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)

    evidence = {
        "result": "PASS",
        "purpose": "Windows生成最终Reference候选",
        "commit_sha": os.environ["GITHUB_SHA"],
        "workflow_run_id": os.environ["GITHUB_RUN_ID"],
        "runner_image": os.environ["ImageOS"],
        "windows_host": os.environ["ALE_WINDOWS_HOST"],
        "wsl_version": os.environ["ALE_WSL_VERSION"],
        "linux_distribution": platform.platform(),
        "python_version": platform.python_version(),
        "apache_airflow_version": subprocess.run(["airflow", "version"], text=True, capture_output=True, env=env, check=True).stdout.strip(),
        "network_guard": {"enabled": True, "external_connections_allowed": False, "loopback_allowed": True},
        "generated_paths": sorted(paths),
        "input_package_sha256": sha256(ROOT / "task" / "输入数据包.zip"),
        "reference_candidate_sha256": sha256(package),
        "migrate_exit_code": migrate.returncode,
        "process_exit_code": process.returncode,
    }
    (ROOT / "reference-generation.json").write_text(json.dumps(evidence, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
