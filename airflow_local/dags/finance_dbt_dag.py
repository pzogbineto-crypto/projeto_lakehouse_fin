from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# 1. Configurações padrão da nossa DAG
default_args = {
    'owner': 'pedro_zogbi',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Data fictícia de início
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Definindo a DAG e o Agendamento (Schedule)
with DAG(
    'finance_lakehouse_transformation',
    default_args=default_args,
    description='DAG para orquestrar a transformação de dados financeiros no BigQuery',
    schedule_interval='0 6 * * *', # Expressão Cron: Roda todo dia às 06:00 AM
    catchup=False,
    tags=['finance', 'dbt', 'bigquery'],
) as dag:

    # 3. Nossas Tarefas (Tasks)
    start_pipeline = EmptyOperator(task_id='start_pipeline')
    
    run_dbt_staging = EmptyOperator(task_id='run_dbt_silver_layer')
    
    test_dbt_staging = EmptyOperator(task_id='test_data_quality_silver')
    
    run_dbt_marts = EmptyOperator(task_id='run_dbt_gold_layer')
    
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # SEU DESAFIO AQUI 👇
    # Como ligamos essas tarefas na ordem correta usando o operador ">>"?
    # A ordem lógica deve ser: Iniciar -> Rodar Silver -> Testar Silver -> Rodar Gold -> Fim
    start_pipeline >> run_dbt_staging >> test_dbt_staging >> run_dbt_marts >> end_pipeline