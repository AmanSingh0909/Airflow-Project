from airflow.sdk import dag, task

@dag
def xcoms_dags_manual():

    @task.python
    def first_task(**kwargs):

        # Extracting task instance'ti' from kwargs to push XCOMS manually 

        ti = kwargs['ti']

        print("Extracting data... This is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key= "return_value", value= fetched_data)

    @task.python
    def second_task(**kwargs):
        
        ti = kwargs['ti']

        # pulling XCOMS manually using task instance'ti' from kwargs
        fetched_data = ti.xcom_pull(task_ids= "first_task", key= "return_value")["data"]
        print("Transforming data... This is the second task")

        transformed_data = fetched_data * 2
        transformed_data_dict = {"transf_data": transformed_data}
        ti.xcom_push(key= 'return_value', value= transformed_data_dict)

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        load_data = ti.xcom_pull(task_ids= "second_task", key= "return_value")
        return load_data

    # Defining task dependencies

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third


# instantiating the DAG
xcoms_dags_manual()