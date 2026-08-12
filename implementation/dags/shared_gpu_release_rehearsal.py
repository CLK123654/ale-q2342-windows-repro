from __future__ import annotations

import csv
import json
import os
import re
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def input_root() -> Path:
    value = os.environ.get("REHEARSAL_INPUT_ROOT")
    if not value:
        raise AirflowException("REHEARSAL_INPUT_ROOT is required")
    return Path(value)


def output_root() -> Path:
    value = os.environ.get("REHEARSAL_OUTPUT_ROOT")
    if not value:
        raise AirflowException("REHEARSAL_OUTPUT_ROOT is required")
    return Path(value)


def read_csv(name: str) -> list[dict[str, str]]:
    with (input_root() / name).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_policy() -> dict:
    return json.loads((input_root() / "release_policy.json").read_text(encoding="utf-8"))


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def event_path(scenario_id: str, event_order: int, event_type: str, object_id: str) -> Path:
    folder = output_root() / ".runtime_events" / scenario_id
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{event_order:03d}_{event_type.lower()}_{slug(object_id)}.json"


def write_event(scenario_id: str, event_order: int, event_type: str, object_id: str, detail: str) -> None:
    payload = {
        "scenario_id": scenario_id,
        "event_order": event_order,
        "event_type": event_type,
        "object_id": object_id,
        "detail": detail,
    }
    event_path(scenario_id, event_order, event_type, object_id).write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def acquire_release_lease(**context) -> None:
    policy = read_policy()
    scenario_id = context["dag_run"].conf["scenario_id"]
    write_event(scenario_id, int(policy["lease_acquired_order"]), "LEASE_ACQUIRED", policy["lease_name"], policy["lease_acquired_detail"])


def verify_request(request: dict[str, str], **context) -> None:
    scenario_id = context["dag_run"].conf["scenario_id"]
    fail_request_id = context["dag_run"].conf.get("fail_request_id") or ""
    if request["request_id"] == fail_request_id:
        raise AirflowException(read_policy()["failure_message"])
    write_event(
        scenario_id,
        int(request["release_order"]),
        "REQUEST_VERIFIED",
        request["request_id"],
        request["event_detail"],
    )


def release_release_lease(**context) -> None:
    policy = read_policy()
    dag_run = context["dag_run"]
    scenario_id = dag_run.conf["scenario_id"]
    write_event(scenario_id, int(policy["lease_released_order"]), "LEASE_RELEASED", policy["lease_name"], policy["lease_released_detail"])
    failed_requests = sorted(
        instance.task_id
        for instance in dag_run.get_task_instances()
        if instance.task_id.startswith("request_") and str(instance.state) == "failed"
    )
    if failed_requests:
        raise AirflowException("verification tasks failed after lease release: " + ",".join(failed_requests))


requests = sorted(read_csv("training_requests.csv"), key=lambda row: (int(row["release_order"]), row["request_id"]))
policy = read_policy()

with DAG(
    dag_id=policy["dag_id"],
    schedule=policy["schedule"],
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=policy["catchup"],
    max_active_runs=int(policy["max_active_runs"]),
    tags=["gpu-release", "rehearsal"],
) as dag:
    acquire = PythonOperator(
        task_id=policy["setup_task_id"],
        python_callable=acquire_release_lease,
    ).as_setup()

    groups = []
    for request in requests:
        with TaskGroup(group_id=request["task_group_id"]) as request_group:
            PythonOperator(
                task_id=policy["request_task_id"],
                python_callable=verify_request,
                op_kwargs={"request": request},
                pool=request["pool_name"],
                pool_slots=int(request["pool_slots"]),
            )
        groups.append(request_group)

    acquire >> groups[0]
    for previous, following in zip(groups, groups[1:]):
        previous >> following

    release = PythonOperator(
        task_id=policy["teardown_task_id"],
        python_callable=release_release_lease,
    ).as_teardown(setups=acquire, on_failure_fail_dagrun=True)

    groups[-1] >> release
