from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.task_group import TaskGroup

import json
import pandas as pd

with DAG(dag_id='airbyte_dynamic_dag',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
         ) as dag:
    
    json_data = """
                [
                {"connectionId": "81b3ec09-7a31-4d86-a31e-28f958090a88", "name": "Postgres_to_BigQuery_person_person"},
                {"connectionId": "66a2d988-f516-4c7a-96c0-9db8c82d5328", "name": "Postgres_to_BigQuery_sales_store"},
                {"connectionId": "bfd5fcf1-a2ff-4070-ad86-16fcf67589cb", "name": "Postgres_to_BigQuery_sales_salesorderdetail"},
                {"connectionId": "7b8859f7-09ca-4b30-9ecf-a42ddf08fc24", "name": "Postgres_to_BigQuery_person_adress"},
                {"connectionId": "4370cd62-359e-4eba-9ee8-b4cea392cee8", "name": "Postgres_to_BigQuery_sales_creditcard"},
                {"connectionId": "4e5763e3-93bf-42f2-bfcd-2875f6cc0c50", "name": "Postgres_to_BigQuery_production_product"},
                {"connectionId": "e18220f1-d05b-4606-b1b1-5c5c70edd44b", "name": "Postgres_to_BigQuery_person_countryregion"},
                {"connectionId": "fce44b76-dc7c-4f56-9402-ed35b0f1b7c8", "name": "Postgres_to_BigQuery_sales_customer"},
                {"connectionId": "7052c161-8aa5-49f8-8bd1-6c493b521bc0", "name": "Postgres_to_BigQuery_sales_salesorderheader"},
                {"connectionId": "fae67a04-78a5-45a1-aa52-c73bae56ba7d", "name": "Postgres_to_BigQuery_sales_salesorderheadersalesreason"},
                {"connectionId": "23ca1389-954d-43a4-a61d-595b233a84d7", "name": "Postgres_to_BigQuery_person_stateprovince"}
                ]
                
                """
    
    json01 = json.dumps(json.loads(json_data))


    airbyte_list = pd.read_json(json01)

    PARAMS_FILE = airbyte_list

    start = DummyOperator(task_id='Inicio')
    end = DummyOperator(task_id='Fim')

    with TaskGroup("airbyte") as ab:
        for indes, row in PARAMS_FILE.iterrows():
            t = AirbyteTriggerSyncOperator(
                task_id=row[1],
                airbyte_conn_id='airbyte_conn_id',
                connection_id=row[0],
                asynchronous=False,
                timeout=3600,
                wait_seconds=3
            )

    start >> ab >> end