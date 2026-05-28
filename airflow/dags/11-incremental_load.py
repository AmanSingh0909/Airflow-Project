from airflow.sdk import dag, task
from pendulum import datetime  
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
        schedule = CronDataIntervalTimetable("@daily",timezone="Asia/Kolkata"), # This means the DAG will run every day at midnight
        start_date = datetime(year=2026, month=5, day=26, tz= "Asia/Kolkata"),
        end_date = datetime(year=2026, month=5, day=30, tz= "Asia/Kolkata"),
        catchup= True,
)
def incremental_load_dag():

    @task.python
    def incremental_data_fetch(**kwargs):
        date_internval_start = kwargs["data_interval_start"]
        date_internval_end = kwargs["data_interval_end"]
        print(f"Fetching incremental data from {date_internval_start} to {date_internval_end}")


    @task.bash
    def inceremental_data_process():
        return "echo 'Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}'"
    


    # Defining task dependencies
    fetch_task = incremental_data_fetch()
    process_task = inceremental_data_process()
    fetch_task >> process_task

# instantiating the DAG
incremental_load_dag()