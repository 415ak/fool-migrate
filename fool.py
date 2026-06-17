from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.empty import EmptyOperator
import pendulum 
import requests
import json
import math
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
import pytz
from psycopg2.extras import execute_values

# disaster access token
#token = "ElA6PJscJ665IMTkJTP2CnbAdhfrbZvlBTPVeKeho17MBVgVGxG7zVZ1Wpnu3mOO"

flood_caption = "1Day"

# remake caption
remake = "test"

catalog_processing_id = Variable.get("DISASTER_CATALOG_ID")
token = Variable.get("DISASTER_API_TOKEN")
conn_id = Variable.get("DISASTER_CONN_ID", default_var="farmai_conn")
limit = 1000
offset = 0

#send line notify
dag_id_to_trigger = "trigger_traget_line_notify"


def get_content(url):
    try: 
        response = requests.get(f"{url}") 
        
        if response.status_code != 200:
            return None
        else:
            return response.json()
    
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
        return None
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
        return None

def process_request(path):
    start = 0 
    while start < 3:
        content = get_content(path) 
        if content is not None:
            return content
        start += 1

    return False
    
    
def total_request(n_max, offset):
    total = 0
    total = int(n_max/offset)
    if n_max % offset:
        total += 1
    
    return total 

def get_content_disaster(url): 
    res = process_request(url)
    
    if type(res) is not dict:
        raise AirflowSkipException
    else:
        #print(res['links'])
        if 'links' in res:
            for link in res['links']:
                if 'rel' in link and link['rel'] == 'next':
                    #print(f"next link: {link['href']}")
                    data_next = get_content_disaster(link['href'])
                    #print(f"data next link: {data_next}")
                    if type(data_next) is dict:
                        res['numberReturned'] += int(data_next['numberReturned'])
                        res['features'].extend(data_next['features'])
                        break
    
    return res        
 
def get_check(sql):
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    
    # Create a cursor object
    cur = conn.cursor()
    
    cur.execute(sql)
    
    # Fetch all rows from the executed query
    rows = cur.fetchall()
    
    cur.close()
    conn.close()
    
    length = len(rows)
    #print(type(length))
    #print(rows)
    
    if length <= 0:
        return None
    else:
        return rows
    
    
# call function task
def call_disaster_api(**kwargs):
    limit_list = limit
    offset_list = offset
    url = f"https://disaster-vallaris.gistda.or.th/core/api/features/1.0/collections/{catalog_processing_id}/items?api_key={token}&limit={limit_list}&offset={offset_list}"
    #print(url)
    res = get_content_disaster(url)
    
    if type(res) is not dict:
        raise AirflowSkipException 
    else:
        print(f"{res['numberMatched']}, {res['numberReturned']}")
        
        #res['numberMatched'] = 1
        #res['numberReturned'] = 1
        
        if res['numberMatched'] == res['numberReturned']:
            print(f"data: {res}")
            if 'features' in res:
                print(f"responce data: {res}")
                return res
            else:
                return None
        else:
            raise AirflowSkipException
        
def error_call_api(**kwargs):
    ti = kwargs['ti']
    res = ti.xcom_pull(task_ids='call_disaster_api')
    #print(f"error_read_file: {type(res)}")
    if type(res) is dict:
        raise AirflowSkipException

        
def check_data_list(**kwargs): 
    ti = kwargs['ti']
    data_list = ti.xcom_pull(task_ids='call_disaster_api')
    #print(f"check_data_list: {data_list}") 
    
    if type(data_list) is not dict:
        raise AirflowSkipException
        

def inset_data_list(**kwargs):
    ti = kwargs['ti']
    data_list = ti.xcom_pull(task_ids='call_disaster_api')
    print(f"save_data: {data_list}") 
    
    table_name = "disaster_floods"
    shpname = flood_caption
    
    current_time = datetime.now()
    timezone = pytz.timezone('Asia/Bangkok')
    date_time = current_time.astimezone(timezone) 
    
    if "features" in data_list:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        rows_to_insert = []
        keys_array = []

        for feature in data_list['features']:
            if "properties" in feature:
                properties = feature['properties']
                
                # Prepare data for insertion
                properties['_collectionid'] = properties.pop('_collectionId', None)
                properties['created_at'] = int(date_time.timestamp() * 1000)
                properties['geom'] = json.dumps(feature['geometry'])
                properties['shpname'] = shpname
                properties['remake'] = "airflow_schedule"
                
                if not keys_array:
                    keys_array = list(properties.keys())
                
                rows_to_insert.append(tuple(properties[key] for key in keys_array))

        if rows_to_insert:
            strKey = ",".join([f"\"{key}\"" for key in keys_array])
            template = "(" + ",".join(["ST_GeomFromGeoJSON(%s)" if key == 'geom' else "%s" for key in keys_array]) + ")"
            # Using ON CONFLICT (_id) DO NOTHING for efficient bulk insertion
            sql = f"INSERT INTO {table_name} ({strKey}) VALUES %s ON CONFLICT (_id) DO NOTHING"

            execute_values(cur, sql, rows_to_insert, template=template)
            conn.commit()

        cur.close()
        conn.close()
        return "Success."
    else:
        return None
    

def save_value_fail(**kwargs):
    ti = kwargs['ti']
    list = ti.xcom_pull(task_ids='inset_data_list')
    
    if list is not None:
        raise AirflowSkipException  
    
    
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 11),
    'retries': 1
}

with DAG(
    "Flood_1day",
    schedule = '@daily',
    #schedule= '0 2 * * *',
    catchup=False,
    start_date = pendulum.datetime(2024, 1, 1, tz="Asia/Bangkok"),
    tags=["Floods"]
) as dag:

    start = EmptyOperator(
        task_id='start',
        trigger_rule="one_success",
    )

    call_disaster_api_task = PythonOperator(
        task_id='call_disaster_api',
        python_callable=call_disaster_api,
    )

    error_call_api_task = PythonOperator(
        task_id='error_call_api',
        python_callable=error_call_api,
    )

    trigger_error_call_api = TriggerDagRunOperator( # Operator ที่ใช่ในการ trigger pipeline อื่น
        task_id="trigger_error_call_api",
        trigger_dag_id=dag_id_to_trigger, # ID ของ DAG เป้าหมายที่ต้องการจะ Trigger
        conf={"message": f"Error: Call Api Disaster Flood {flood_caption}."}, # เราสามารถส่ง paramter ข้าม pipeline ไปรันใน pipeline ที่ถูก trigger
    )

    check_data_list_task = PythonOperator(
        task_id='check_data_list',
        python_callable=check_data_list,
    )

    inset_data_list_task = PythonOperator(
        task_id='inset_data_list',
        python_callable=inset_data_list,
    )

    save_value_fail_task = PythonOperator(
        task_id='save_value_fail',
        python_callable=save_value_fail,
    )

    trigger_error_save = TriggerDagRunOperator( # Operator ที่ใช่ในการ trigger pipeline อื่น
        task_id="trigger_error_save",
        trigger_dag_id=dag_id_to_trigger, # ID ของ DAG เป้าหมายที่ต้องการจะ Trigger
        conf={"message": f"Error: Save Disaster Flood {flood_caption}."}, # เราสามารถส่ง paramter ข้าม pipeline ไปรันใน pipeline ที่ถูก trigger
    )


    end = EmptyOperator(
        task_id='end',
        trigger_rule="one_success",
    )


    start >> call_disaster_api_task

    call_disaster_api_task >> check_data_list_task >> inset_data_list_task >> end

    inset_data_list_task >> save_value_fail_task >> trigger_error_save >> end

    call_disaster_api_task >> error_call_api_task >> trigger_error_call_api >> end