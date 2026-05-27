from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
        dag_id= "delta_schedule_dag",
        start_date = datetime(year=2026, month=5, day=26, tz= "Asia/Kolkata"),
        schedule = DeltaTriggerTimetable(duration(days=3)), # This means the DAG will run every day
        end_date = datetime(year=2026, month=5, day=30, tz= "Asia/Kolkata"),
        is_paused_upon_creation= False,
        catchup= True
)
def delta_schedule_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def secound_task():
        print("This is the secound task")

    @task.python
    def third_task():
        print("This is the third task. Dag is complete")


    # Defining task dependencies

    first = first_task()
    secound = secound_task()
    third = third_task()

    first >> secound >> third


# instantiating the DAG
delta_schedule_dag()