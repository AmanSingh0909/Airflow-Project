from airflow.sdk import dag, task

@dag
def xcoms_dags_auto():

    @task.python
    def first_task():
        print("Extracting data... This is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task.python
    def secound_task(data: dict):
        fetched_data = data['data']
        transformed_data = fetched_data * 2
        transformed_data_dict = {"transf_data": transformed_data}
        return transformed_data_dict

    @task.python
    def third_task(data: dict):
        load_data = data
        return load_data

    # Defining task dependencies

    first = first_task()
    second = secound_task(first)
    third = third_task(second)



# instantiating the DAG
xcoms_dags_auto()