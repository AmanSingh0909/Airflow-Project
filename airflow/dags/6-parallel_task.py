from airflow.sdk import dag, task

@dag
def parallel_task_dag():

    @task.python
    def extract_task(**kwargs):
        print("This is the first task")
        ti = kwargs['ti']
        extracted_data = {"api_extracted_data" : [1,2,3],
                          "db_extracted_data" : [4,5,6],
                          "s3_extracted_data" : [7,8,9]}
        
        ti.xcom_push(key= "return_value", value= extracted_data)

    @task.python
    def transform_task_api(**kwargs):
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(task_ids= "extract_task")["api_extracted_data"]
        print(f"Transforming API.....: {api_extracted_data}")
        tranformed_api_data = [i*10 for i in api_extracted_data]
        ti.xcom_push(key= "return_value", value= tranformed_api_data)

    @task.python
    def transform_task_db(**kwargs):
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(task_ids= "extract_task")["db_extracted_data"]
        print(f"Transforming DB.....: {db_extracted_data}")
        tranformed_db_data = [i*100 for i in db_extracted_data]
        ti.xcom_push(key= "return_value", value= tranformed_db_data)

    @task.python
    def transform_task_s3(**kwargs):
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(task_ids= "extract_task")["s3_extracted_data"]
        print(f"Transforming S3.....: {s3_extracted_data}")
        tranformed_s3_data = [i*1000 for i in s3_extracted_data]
        ti.xcom_push(key= "return_value", value= tranformed_s3_data)

    @task.bash
    def load_task(**kwargs):
        print("loading data to destination......")
        api_data = kwargs['ti'].xcom_pull(task_ids= "transform_task_api")
        db_data = kwargs['ti'].xcom_pull(task_ids= "transform_task_db")
        s3_data = kwargs['ti'].xcom_pull(task_ids= "transform_task_s3")

        return f"echo 'loaded data: {api_data}, {db_data}, {s3_data}'"
    
    # Defining task dependencies
    extract = extract_task()
    transform_api = transform_task_api()
    transform_db = transform_task_db()
    transform_s3 = transform_task_s3()
    load = load_task()

    extract >> [transform_api, transform_db, transform_s3] >> load


# instantiating the DAG
parallel_task_dag()