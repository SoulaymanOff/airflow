# dags/demo_etl.py
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

@dag(
    schedule=None,                    # run only when you click "Trigger"
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["demo"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=10)},
)
def demo_etl():
    @task
    def extract():
        # Pretend we pulled rows from somewhere
        return ["alpha", "bravo", "charlie", "delta"]

    @task
    def pick_limit_from_conf():
        # Read JSON conf passed at trigger time, fallback to 2
        ctx = get_current_context()
        return int(ctx["dag_run"].conf.get("limit", 2))

    @task
    def transform(rows: list[str], limit: int):
        # Keep first N rows and uppercase them
        return [r.upper() for r in rows[:limit]]

    @task
    def quality_check(rows: list[str]) -> int:
        assert rows, "No rows after transform!"
        return len(rows)

    @task
    def load(rows: list[str], count: int):
        print(f"Loading {count} rows: {rows}")

    raw = extract()
    limit = pick_limit_from_conf()
    cleaned = transform(raw, limit)
    count = quality_check(cleaned)
    load(cleaned, count)

dag = demo_etl()
