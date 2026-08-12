# Airflow GPU release rehearsal

This public repository contains one self-contained task for a shared GPU training release rehearsal. The task uses Apache Airflow2.10.5 on a Windows11 host through WSL2, imports the final DAG with DagBag, writes two Pool records, runs two DagRun scenarios, and exports TaskInstance and lease-event handoff reports.

The workflow runs on windows-2025. Dependency installation is separated from formal execution. During formal execution, Python network connections are restricted to local addresses. The verifier rebuilds the complete Reference in two clean directories twice, changes one Pool capacity and observes the corresponding Airflow result, then checks that a duplicate request fails without leaving an output directory.

The task materials are synthetic and contain no production identifiers, credentials, customer data, or private Lark metadata.
