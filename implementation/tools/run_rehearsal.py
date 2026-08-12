from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
from pathlib import Path

import pendulum
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.models.pool import Pool
from airflow.utils.session import create_session


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def fail(message: str) -> None:
    raise ValueError(message)


def validate_inputs(input_root: Path) -> tuple[list[dict], list[dict], list[dict], list[dict], dict, list[dict]]:
    requests = read_csv(input_root / "training_requests.csv")
    teams = read_csv(input_root / "team_controls.csv")
    pools = read_csv(input_root / "pool_plan.csv")
    scenarios = read_csv(input_root / "rehearsal_scenarios.csv")
    report_contract = read_csv(input_root / "report_contract.csv")
    policy = json.loads((input_root / "release_policy.json").read_text(encoding="utf-8"))

    def unique(rows: list[dict], key: str) -> None:
        values = [row[key] for row in rows]
        if len(values) != len(set(values)):
            fail(f"duplicate {key}")

    unique(requests, "request_id")
    unique(teams, "team_id")
    unique(pools, "pool_name")
    unique(scenarios, "scenario_id")
    unique(report_contract, "report_path")
    team_by_id = {row["team_id"]: row for row in teams}
    pool_by_name = {row["pool_name"]: row for row in pools}
    for pool in pools:
        if pool["include_deferred"] not in {"true", "false"}:
            fail(f"invalid include_deferred for {pool['pool_name']}")
    request_ids = {row["request_id"] for row in requests}
    for request in requests:
        team = team_by_id.get(request["team_id"])
        if not team:
            fail(f"unknown team for {request['request_id']}")
        if request["pool_name"] != team["allowed_pool"]:
            fail(f"pool policy mismatch for {request['request_id']}")
        if request["pool_name"] not in pool_by_name:
            fail(f"unknown pool for {request['request_id']}")
        slots = int(request["pool_slots"])
        if slots < 1 or slots > int(team["max_pool_slots"]):
            fail(f"pool slot limit exceeded for {request['request_id']}")
        expected_detail = "|".join(
            [
                f"team={request['team_id']}",
                f"owner={team['release_owner']}",
                f"model={request['model_family']}",
                f"slice={request['dataset_slice']}",
                f"pool={request['pool_name']}",
                f"slots={request['pool_slots']}",
            ]
        )
        if request["event_detail"] != expected_detail:
            fail(f"event detail mismatch for {request['request_id']}")
    for scenario in scenarios:
        fail_id = scenario["fail_request_id"]
        if fail_id and fail_id not in request_ids:
            fail(f"unknown failure request for {scenario['scenario_id']}")
        if scenario["expected_dag_state"] not in {"success", "failed"}:
            fail(f"invalid expected state for {scenario['scenario_id']}")
    required = {"dag_id", "setup_task_id", "teardown_task_id", "request_task_id", "lease_name", "schedule", "max_active_runs", "catchup", "failure_message", "teardown_trigger_rule", "lease_acquired_order", "lease_released_order", "lease_acquired_detail", "lease_released_detail", "required_event_types"}
    if set(policy) != required:
        fail("release policy keys are incomplete")
    expected_reports = {
        "reports/dag_contract.csv",
        "reports/pool_configuration.csv",
        "reports/run_outcomes.csv",
        "reports/task_instance_evidence.csv",
        "reports/lease_events.csv",
    }
    if {row["report_path"] for row in report_contract} != expected_reports:
        fail("report contract paths are incomplete")
    for row in report_contract:
        if not row["field_order"] or not row["business_key"] or not row["consumer"]:
            fail(f"report contract is incomplete for {row['report_path']}")
    return requests, teams, pools, scenarios, policy, report_contract


def contract_fields(report_contract: list[dict], report_path: str) -> list[str]:
    row = next(item for item in report_contract if item["report_path"] == report_path)
    return row["field_order"].split("|")


def install_pools(pools: list[dict]) -> list[dict]:
    with create_session() as session:
        for row in pools:
            current = session.query(Pool).filter(Pool.pool == row["pool_name"]).one_or_none()
            if current is None:
                current = Pool(pool=row["pool_name"], slots=int(row["slots"]), description=row["description"], include_deferred=row["include_deferred"] == "true")
                session.add(current)
            else:
                current.slots = int(row["slots"])
                current.description = row["description"]
                current.include_deferred = row["include_deferred"] == "true"
    names = {row["pool_name"] for row in pools}
    with create_session() as session:
        actual = session.query(Pool).filter(Pool.pool.in_(names)).all()
        return [
            {
                "pool_name": row.pool,
                "slots": row.slots,
                "include_deferred": str(bool(row.include_deferred)).lower(),
                "description": row.description,
            }
            for row in sorted(actual, key=lambda item: item.pool)
        ]


def dag_contract(dag) -> list[dict]:
    rows = []
    for task in sorted(dag.tasks, key=lambda item: item.task_id):
        rows.append(
            {
                "task_id": task.task_id,
                "operator": task.__class__.__name__,
                "upstream_task_ids": "|".join(sorted(task.upstream_task_ids)),
                "downstream_task_ids": "|".join(sorted(task.downstream_task_ids)),
                "pool": task.pool,
                "pool_slots": task.pool_slots,
                "is_setup": str(bool(task.is_setup)).lower(),
                "is_teardown": str(bool(task.is_teardown)).lower(),
                "trigger_rule": str(task.trigger_rule),
            }
        )
    return rows


def query_run(dag_id: str, logical_date) -> tuple[DagRun, list[TaskInstance]]:
    with create_session() as session:
        run = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date == logical_date).one()
        instances = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run.run_id).all()
        session.expunge(run)
        for instance in instances:
            session.expunge(instance)
    return run, sorted(instances, key=lambda item: (item.task_id, item.map_index))


def collect_events(root: Path) -> list[dict]:
    rows = []
    for path in sorted((root / ".runtime_events").glob("*/*.json")):
        rows.append(json.loads(path.read_text(encoding="utf-8")))
    return sorted(rows, key=lambda row: (row["scenario_id"], int(row["event_order"]), row["event_type"], row["object_id"]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--dag-file", required=True)
    args = parser.parse_args()
    input_root = Path(args.input).resolve()
    output_root = Path(args.output).resolve()
    dag_file = Path(args.dag_file).resolve()
    if output_root.exists():
        fail("output directory must not exist")
    requests, teams, pools, scenarios, policy, report_contract = validate_inputs(input_root)
    output_root.mkdir(parents=True)
    os.environ["REHEARSAL_INPUT_ROOT"] = str(input_root)
    os.environ["REHEARSAL_OUTPUT_ROOT"] = str(output_root)
    pool_rows = install_pools(pools)

    deliverable_dag = output_root / "dags" / "shared_gpu_release_rehearsal.py"
    deliverable_tool = output_root / "tools" / "run_rehearsal.py"
    deliverable_dag.parent.mkdir(parents=True)
    deliverable_tool.parent.mkdir(parents=True)
    shutil.copy2(dag_file, deliverable_dag)
    shutil.copy2(Path(__file__).resolve(), deliverable_tool)

    bag = DagBag(dag_folder=str(deliverable_dag.parent), include_examples=False, safe_mode=False)
    if bag.import_errors:
        fail("DagBag import errors: " + json.dumps(bag.import_errors, sort_keys=True))
    dag = bag.get_dag(policy["dag_id"])
    if dag is None:
        fail("required DAG is missing")
    teardown = dag.get_task(policy["teardown_task_id"])
    if str(teardown.trigger_rule) != policy["teardown_trigger_rule"]:
        fail("teardown trigger rule does not match release policy")

    contract_rows = dag_contract(dag)
    write_csv(output_root / "reports" / "pool_configuration.csv", contract_fields(report_contract, "reports/pool_configuration.csv"), pool_rows)
    write_csv(
        output_root / "reports" / "dag_contract.csv",
        contract_fields(report_contract, "reports/dag_contract.csv"),
        contract_rows,
    )

    outcome_rows = []
    instance_rows = []
    for scenario in scenarios:
        logical_date = pendulum.parse(scenario["logical_date"])
        conf = {"scenario_id": scenario["scenario_id"], "fail_request_id": scenario["fail_request_id"]}
        try:
            dag.test(execution_date=logical_date, run_conf=conf, use_executor=False)
        except Exception:
            pass
        run, instances = query_run(policy["dag_id"], logical_date)
        events = [row for row in collect_events(output_root) if row["scenario_id"] == scenario["scenario_id"]]
        released = sum(row["event_type"] == "LEASE_RELEASED" for row in events)
        outcome_rows.append(
            {
                "scenario_id": scenario["scenario_id"],
                "logical_date": scenario["logical_date"],
                "expected_dag_state": scenario["expected_dag_state"],
                "observed_dag_state": str(run.state),
                "fail_request_id": scenario["fail_request_id"],
                "lease_released_count": released,
            }
        )
        for instance in instances:
            task = dag.get_task(instance.task_id)
            instance_rows.append(
                {
                    "scenario_id": scenario["scenario_id"],
                    "task_id": instance.task_id,
                    "map_index": instance.map_index,
                    "state": str(instance.state),
                    "try_number": instance.try_number,
                    "pool": task.pool,
                    "pool_slots": task.pool_slots,
                }
            )

    event_rows = collect_events(output_root)
    required_event_types = set(policy["required_event_types"])
    for scenario in scenarios:
        scenario_events = [row for row in event_rows if row["scenario_id"] == scenario["scenario_id"]]
        observed_types = {row["event_type"] for row in scenario_events}
        if observed_types != required_event_types:
            fail(f"event types do not match release policy for {scenario['scenario_id']}")
        for event_type in ("LEASE_ACQUIRED", "LEASE_RELEASED"):
            if sum(row["event_type"] == event_type for row in scenario_events) != 1:
                fail(f"lease event count is invalid for {scenario['scenario_id']} and {event_type}")
    write_csv(
        output_root / "reports" / "run_outcomes.csv",
        contract_fields(report_contract, "reports/run_outcomes.csv"),
        outcome_rows,
    )
    write_csv(
        output_root / "reports" / "task_instance_evidence.csv",
        contract_fields(report_contract, "reports/task_instance_evidence.csv"),
        instance_rows,
    )
    write_csv(
        output_root / "reports" / "lease_events.csv",
        contract_fields(report_contract, "reports/lease_events.csv"),
        event_rows,
    )
    shutil.rmtree(output_root / ".runtime_events")

    expected = {row["scenario_id"]: row["expected_dag_state"] for row in scenarios}
    for row in outcome_rows:
        if row["observed_dag_state"] != expected[row["scenario_id"]]:
            fail(f"unexpected DagRun state for {row['scenario_id']}")
        if row["lease_released_count"] != 1:
            fail(f"lease did not converge for {row['scenario_id']}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise
