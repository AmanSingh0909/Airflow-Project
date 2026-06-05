from airflow.sdk import dag, task

@dag
def first_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def secound_task():
        print("This is the secound task for tranformation")

    @task.python
    def third_task():
        print("This is the third task. Dag is complete and loaded data to the database")


    # Defining task dependencies

    first = first_task()
    secound = secound_task()
    third = third_task()

    first >> secound >> third


# instantiating the DAG
first_dag()