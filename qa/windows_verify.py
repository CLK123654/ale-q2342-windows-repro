from __future__ import annotations

import csv
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
TASK = ROOT / "task"
EVIDENCE = ROOT / "evidence"
RUN_ROOT = ROOT / ".qa-run"


def run(command: list[str], env: dict[str, str], timeout: int = 300) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, text=True, capture_output=True, check=False, timeout=timeout, env=env)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def tree(root: Path) -> dict[str, str]:
    return {path.relative_to(root).as_posix(): sha(path) for path in sorted(root.rglob("*")) if path.is_file()}


def normalized_reference_tree(root: Path) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix()
        if "__pycache__" in path.parts or path.suffix == ".pyc":
            continue
        data = path.read_bytes()
        if relative.endswith((".csv", ".py")):
            data = data.replace(b"\r\n", b"\n")
        normalized[relative] = hashlib.sha256(data).hexdigest()
    return normalized


def reset(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def extract(archive: Path, target: Path) -> None:
    target.mkdir(parents=True)
    with zipfile.ZipFile(archive) as package:
        package.extractall(target)


def airflow_env(home: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(
        {
            "AIRFLOW_HOME": str(home),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__FERNET_KEY": "",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{home / 'airflow.db'}",
            "ALE_NETWORK_GUARD": "1",
            "PYTHONPATH": str(ROOT / "qa" / "network_guard"),
        }
    )
    return env


def build(input_root: Path, output_root: Path, dag_file: Path, home: Path) -> tuple[subprocess.CompletedProcess[str], subprocess.CompletedProcess[str]]:
    env = airflow_env(home)
    migrate = run(["airflow", "db", "migrate"], env)
    if migrate.returncode != 0:
        return migrate, migrate
    process = run(
        [
            sys.executable,
            str(ROOT / "implementation" / "tools" / "run_rehearsal.py"),
            "--input",
            str(input_root / "input_data"),
            "--output",
            str(output_root),
            "--dag-file",
            str(dag_file),
        ],
        env,
    )
    return migrate, process


def main() -> None:
    reset(EVIDENCE)
    reset(RUN_ROOT)
    expected_hashes = json.loads((ROOT / "qa" / "expected_hashes.json").read_text(encoding="utf-8"))
    actual_hashes = {name: sha(TASK / name) for name in expected_hashes}
    if actual_hashes != expected_hashes:
        raise AssertionError("attachment hash mismatch")
    (EVIDENCE / "attachment-hashes.json").write_text(json.dumps(actual_hashes, ensure_ascii=False, indent=2), encoding="utf-8")

    version = run(["airflow", "version"], dict(os.environ))
    if version.returncode != 0 or version.stdout.strip() != "2.10.5":
        raise RuntimeError("Apache Airflow2.10.5 is required")
    reference = RUN_ROOT / "reference"
    extract(TASK / "reference.zip", reference)
    reference_tree = normalized_reference_tree(reference)
    clean_runs = []
    for directory_id in ["clean-a", "clean-b"]:
        base = RUN_ROOT / directory_id
        input_root = base / "input"
        extract(TASK / "输入数据包.zip", input_root)
        input_before = tree(input_root)
        dag_file = ROOT / "implementation" / "dags" / "shared_gpu_release_rehearsal.py"
        for process_index in [1, 2]:
            output_root = base / f"output-{process_index}"
            home = base / f"airflow-home-{process_index}"
            migrate, process = build(input_root, output_root, dag_file, home)
            if migrate.returncode != 0 or process.returncode != 0:
                raise RuntimeError(migrate.stdout + migrate.stderr + process.stdout + process.stderr)
            output_tree = normalized_reference_tree(output_root)
            if output_tree != reference_tree:
                mismatch = {
                    key: {"reference": reference_tree.get(key), "output": output_tree.get(key)}
                    for key in sorted(set(reference_tree) | set(output_tree))
                    if reference_tree.get(key) != output_tree.get(key)
                }
                (EVIDENCE / "reference-mismatch.json").write_text(json.dumps(mismatch, ensure_ascii=False, indent=2), encoding="utf-8")
                raise AssertionError(f"Reference mismatch in {directory_id} process {process_index}: {sorted(mismatch)}")
            clean_runs.append(
                {
                    "directory_id": directory_id,
                    "process_index": process_index,
                    "migrate_exit_code": migrate.returncode,
                    "process_exit_code": process.returncode,
                    "input_unchanged": True,
                    "reference_match": True,
                    "generated_paths": sorted(reference_tree),
                }
            )
        if tree(input_root) != input_before:
            raise AssertionError("input changed during clean run")

    positive = RUN_ROOT / "positive"
    positive_input = positive / "input"
    extract(TASK / "输入数据包.zip", positive_input)
    pool_plan = positive_input / "input_data" / "pool_plan.csv"
    rows = list(csv.DictReader(pool_plan.open(encoding="utf-8", newline="")))
    for row in rows:
        if row["pool_name"] == "gpu_standard":
            row["slots"] = "5"
    with pool_plan.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["pool_name", "slots", "include_deferred", "description"], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    positive_output = positive / "output"
    migrate, process = build(positive_input, positive_output, ROOT / "implementation" / "dags" / "shared_gpu_release_rehearsal.py", positive / "airflow-home")
    if migrate.returncode != 0 or process.returncode != 0:
        raise RuntimeError(migrate.stdout + migrate.stderr + process.stdout + process.stderr)
    pool_rows = list(csv.DictReader((positive_output / "reports" / "pool_configuration.csv").open(encoding="utf-8", newline="")))
    positive_slots = {row["pool_name"]: row["slots"] for row in pool_rows}
    if positive_slots.get("gpu_standard") != "5":
        raise AssertionError("positive input change did not reach Airflow Pool output")
    if normalized_reference_tree(positive_output) == reference_tree:
        raise AssertionError("positive input change did not change business output")
    (EVIDENCE / "positive-case.json").write_text(
        json.dumps({"input_field": "pool_plan.csv gpu_standard.slots", "before": 4, "after": 5, "observed_pool_slots": 5}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    negative_logs = []

    def negative_case(case_id: str, mutate) -> None:
        base = RUN_ROOT / "negative" / case_id
        input_root = base / "input"
        extract(TASK / "输入数据包.zip", input_root)
        mutate(input_root / "input_data")
        output_root = base / "output"
        migrate, process = build(
            input_root,
            output_root,
            ROOT / "implementation" / "dags" / "shared_gpu_release_rehearsal.py",
            base / "airflow-home",
        )
        if migrate.returncode != 0:
            raise RuntimeError(migrate.stdout + migrate.stderr)
        if process.returncode == 0 or output_root.exists():
            raise AssertionError(f"{case_id} did not fail closed")
        negative_logs.append(f"[{case_id}] exit={process.returncode}\n{process.stdout}{process.stderr}")

    def duplicate_request(input_data: Path) -> None:
        path = input_data / "training_requests.csv"
        lines = path.read_text(encoding="utf-8").splitlines()
        path.write_text("\n".join(lines + [lines[1]]) + "\n", encoding="utf-8")

    def update_csv(input_data: Path, filename: str, key: str, value: str) -> None:
        path = input_data / filename
        rows = list(csv.DictReader(path.open(encoding="utf-8", newline="")))
        rows[0][key] = value
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)

    negative_case("duplicate-request", duplicate_request)
    negative_case("unknown-team", lambda root: update_csv(root, "training_requests.csv", "team_id", "unknown"))
    negative_case("pool-mismatch", lambda root: update_csv(root, "training_requests.csv", "pool_name", "gpu_memory"))
    negative_case("slot-limit", lambda root: update_csv(root, "training_requests.csv", "pool_slots", "3"))
    negative_case("unknown-failure-object", lambda root: update_csv(root, "rehearsal_scenarios.csv", "fail_request_id", "REQ-9999"))

    boundary = RUN_ROOT / "negative" / "existing-output"
    boundary_input = boundary / "input"
    extract(TASK / "输入数据包.zip", boundary_input)
    boundary_output = boundary / "output"
    boundary_output.mkdir(parents=True)
    marker = boundary_output / "keep.txt"
    marker.write_text("keep\n", encoding="utf-8")
    migrate, process = build(
        boundary_input,
        boundary_output,
        ROOT / "implementation" / "dags" / "shared_gpu_release_rehearsal.py",
        boundary / "airflow-home",
    )
    if migrate.returncode != 0:
        raise RuntimeError(migrate.stdout + migrate.stderr)
    if process.returncode == 0 or marker.read_text(encoding="utf-8") != "keep\n" or sorted(path.name for path in boundary_output.iterdir()) != ["keep.txt"]:
        raise AssertionError("existing output boundary did not fail closed")
    negative_logs.append(f"[existing-output] exit={process.returncode}\n{process.stdout}{process.stderr}")

    missing = run(
        [
            sys.executable,
            str(ROOT / "implementation" / "tools" / "run_rehearsal.py"),
            "--input",
            str(boundary_input / "input_data"),
            "--output",
            str(RUN_ROOT / "negative" / "missing-parameter-output"),
        ],
        airflow_env(RUN_ROOT / "negative" / "missing-parameter-home"),
    )
    if missing.returncode == 0 or (RUN_ROOT / "negative" / "missing-parameter-output").exists():
        raise AssertionError("missing parameter did not fail closed")
    negative_logs.append(f"[missing-parameter] exit={missing.returncode}\n{missing.stdout}{missing.stderr}")
    (EVIDENCE / "negative-case.log").write_text("\n".join(negative_logs), encoding="utf-8")

    summary = {
        "result": "PASS",
        "commit_sha": os.environ.get("GITHUB_SHA", "local"),
        "workflow_run_id": os.environ.get("GITHUB_RUN_ID", "local"),
        "runner_image": os.environ.get("ImageOS", "local"),
        "windows_host": os.environ.get("ALE_WINDOWS_HOST", "local"),
        "wsl_version": os.environ.get("ALE_WSL_VERSION", "local"),
        "linux_distribution": platform.platform(),
        "python_version": platform.python_version(),
        "main_software": {"name": "Apache Airflow", "version": version.stdout.strip(), "executed": True},
        "network_guard": {"enabled": True, "external_connections_allowed": False, "loopback_allowed": True},
        "attachment_sha256": actual_hashes,
        "reference_path_count": len(reference_tree),
        "clean_directory_count": 2,
        "process_runs_per_directory": 2,
        "clean_runs": clean_runs,
        "positive_mutation": "PASS",
        "negative_case": "PASS",
        "negative_case_count": 7,
    }
    (EVIDENCE / "windows-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
