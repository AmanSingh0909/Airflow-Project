from airflow.sdk import dag, task
from pendulum import datetime  
from airflow.timetables.events import EventsTimetable



special_dates = EventsTimetable(
    event_dates=[
        datetime(2026,5,1),
        datetime(2026,5,15),
        datetime(2026,5,4),
        datetime(2026,5,6)
    ]
)

@dag(
    schedule = special_dates,
    start_date = datetime(year=2026, month=5, day=1, tz= "Asia/Kolkata"),
    end_date = datetime(year=2026, month=5, day=31, tz= "Asia/Kolkata"),
    catchup= True,
    is_paused_upon_creation= False
)

def special_dates_dag():

    @task.python
    def special_event_task(**kwargs):
        execution_date = kwargs["logical_date"]
        print(f"Running task for special event: {execution_date}")

    special_event = special_event_task()


# instantiating the DAG
special_dates_dag()