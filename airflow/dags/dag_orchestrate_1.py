from airflow.sdk import dag, task
import os

@dag
def first_orchestration_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def secound_task():
        print("This is the secound task")

    @task.python
    def third_task():
        # Ensure the directory exists
        os.makedirs(os.path.dirname("/opt/airflow/logs/data"), exist_ok=True)

    # Simulate data processing by writing to a file
        with open("/opt/airflow/logs/data/output_1.txt", "w") as f:
            f.write(f"Data processed successfully")


    # Defining task dependencies

    first = first_task()
    secound = secound_task()
    third = third_task()

    first >> secound >> third


# instantiating the DAG
first_orchestration_dag()