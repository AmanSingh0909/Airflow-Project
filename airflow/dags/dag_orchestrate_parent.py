from dag_orchestrate_1 import first_orchestration_dag
from dag_orchestrate_2 import second_dag
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag
def parent_dag():

    trigger_first_dag = TriggerDagRunOperator(
        task_id="trigger_first_dag",
        trigger_dag_id="first_orchestration_dag",
        wait_for_completion=True # this is optional(this is slow) wait for the triggered DAG to complete before moving to the next task
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id="trigger_second_dag",
        trigger_dag_id="second_dag",
        wait_for_completion=True # this is optional(this is slow) wait for the triggered DAG to complete before moving to the next task
    )

    trigger_first_dag >> trigger_second_dag

# instantiating the DAG
parent_dag()