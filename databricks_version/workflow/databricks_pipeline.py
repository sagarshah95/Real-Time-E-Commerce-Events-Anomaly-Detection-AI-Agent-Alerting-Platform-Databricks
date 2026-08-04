# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
from databricks.sdk.service.jobs import JobSettings as Job


Real_Time_Streaming_Events_Pipeline = Job.from_dict(
    {
        "name": "Real Time Streaming Events Pipeline",
        "tasks": [
            {
                "task_key": "Bronze_Ingestion",
                "depends_on": [
                    {
                        "task_key": "Raw_Data_Generation",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Real Time Streaming Lakehouse with Intelligent Alerting/02_bronze_ingest",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "Raw_Data_Generation",
                "notebook_task": {
                    "notebook_path": "/Workspace/Real Time Streaming Lakehouse with Intelligent Alerting/01_generate_raw_data",
                    "source": "WORKSPACE",
                },
                "disabled": True,
            },
            {
                "task_key": "Silver_Cleaning",
                "depends_on": [
                    {
                        "task_key": "Bronze_Ingestion",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Real Time Streaming Lakehouse with Intelligent Alerting/03_silver_clean_data",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "Gold_KPIs",
                "depends_on": [
                    {
                        "task_key": "Silver_Cleaning",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Real Time Streaming Lakehouse with Intelligent Alerting/04_gold_aggregate",
                    "source": "WORKSPACE",
                },
            },
            {
                "task_key": "Detect_Anomalies",
                "depends_on": [
                    {
                        "task_key": "Gold_KPIs",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Real Time Streaming Lakehouse with Intelligent Alerting/05_anomalies_detection",
                    "source": "WORKSPACE",
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
        "performance_target": "PERFORMANCE_OPTIMIZED",
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Real_Time_Streaming_Events_Pipeline, job_id=1234)
# or create a new job using: w.jobs.create(**Real_Time_Streaming_Events_Pipeline.as_shallow_dict())
