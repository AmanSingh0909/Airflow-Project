from airflow.sdk import dag, task
from pendulum import datetime

@dag(
        dag_id= "first_schedule_dag",
        start_date = datetime(year=2026, month=5, day=1, tz= "Asia/Kolkata"),
        schedule = "@daily",
        is_paused_upon_creation= False,
        catchup= True
)
def first_schedule_dag():

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
first_schedule_dag()