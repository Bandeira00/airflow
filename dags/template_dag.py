from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.papermill.operators.papermill import PapermillOperator

with DAG(
    dag_id='template_dag',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    exec_notebook = PapermillOperator(
    task_id='notebook_test',
    input_nb='/home/gabriel/Documents/proj/analises/notebooks/bike_store_sales_analysis.ipynb',  
    output_nb='/home/gabriel/Documents/proj/analises/notebooks/output/bike_store_sales_analysis_output.ipynb'
)
