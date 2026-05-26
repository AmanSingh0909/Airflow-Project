from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator

@dag
def opterators_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def secound_task():
        print("This is the secound task")

    @task.bash
    def bash_task_modern():
        return "echo https://airflow.apache.org/"
    
    bash_task_oldschool = BashOperator(
        task_id="run_after_loop",
        bash_command="echo https://airflow.apache.org/"
    )


    # Defining task dependencies

    first = first_task()
    secound = secound_task()
    bash_modern = bash_task_modern()
    bash_oldschool = bash_task_oldschool

    first >> secound >> bash_modern >> bash_oldschool


# instantiating the DAG
opterators_dag()