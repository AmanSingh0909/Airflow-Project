from airflow.sdk import dag, task

@dag
def versioned_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def secound_task():
        print("This is the secound task")

    @task.python
    def third_task():
        print("This is the third task. Dag is complete")

    @task.python
    def version_task():
        print("This is the version task. Dag is complete 2.0")


    # Defining task dependencies

    first = first_task()
    secound = secound_task()
    third = third_task()
    version = version_task()

    first >> secound >> third >> version


# instantiating the DAG
versioned_dag()