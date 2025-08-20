from datetime import datetime
from airflow.decorators import dag, task

@dag(
    schedule="@daily",              # cron or presets like @hourly, @daily
    start_date=datetime(2024, 1, 1),
    catchup=False,                  # run only new schedules
    default_args={"retries": 1},
    tags=["demo"],
)
def hello_world():
    @task
    def say_hello():
        print("Hello from Airflow!")

    say_hello()

dag = hello_world()
