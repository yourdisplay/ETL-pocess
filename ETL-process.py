# coding=utf-8

from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функции для CH
def ch_get_df(query='Select 1', host='https://****', user='***', password='***'):
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

def ch_add_df(df, query):
    connection = {
    'host': '***',
    'database':'***',
    'user':'***',
    'password':'***'
    }
    ph.execute(query=query, connection=connection)
    ph.to_clickhouse(df, table='ri_dag_task_6_1', connection=connection, index=False)

query = """SELECT user_id, CountIf(user_id, action = 'like') as likes, CountIf(user_id, action = 'view') as views
            FROM simulator_20230120.feed_actions
            GROUP BY user_id
            ORDER BY user_id ASC
            format TSVWithNames"""

# Запрос для создания таблицы
query_create = """CREATE TABLE IF NOT EXISTS test.ri_dag_task_6_1 (
                date Date,
                dimension String,
                dimension_value String,
                likes UInt64,
                views UInt64,
                messages_received UInt64,
                messages_sent UInt64,
                users_received UInt64,
                users_sent UInt64
                )
                ENGINE = MergeTree()
                ORDER BY date"""

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'ri-g',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 30),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_task_6_1():

    @task()
    def extract_1():
        query = """SELECT toDate(time) as date, user_id, os, gender, age, CountIf(user_id, action = 'like') as likes, CountIf(user_id, action = 'view') as views
                    FROM simulator_20230120.feed_actions
                    WHERE toDate(time) = today() - 1 
                    GROUP BY user_id, os, gender, age, date
                    format TSVWithNames"""
        
        df_cube_1 = ch_get_df(query=query)
        return df_cube_1

    @task()
    def extract_2():
        query = """SELECT date, user_id, t2.reciever_id, os, gender, age, messages_received, messages_sent, users_received, users_sent
                    FROM
                    (SELECT toDate(time) as date, user_id, os, gender, age, COUNT(reciever_id) as messages_received, COUNT(DISTINCT(reciever_id)) as users_received --получено
                    FROM simulator_20230120.message_actions
                    WHERE toDate(time) = today() - 1 
                    GROUP BY user_id, os, gender, age, date) t1
                    INNER JOIN
                    (SELECT toDate(time) as date, reciever_id, COUNT(user_id) as messages_sent, COUNT(DISTINCT(user_id)) as users_sent --отправлено
                    FROM simulator_20230120.message_actions
                    WHERE toDate(time) = today() - 1 
                    GROUP BY reciever_id, date) t2
                    ON t1.user_id = t2.reciever_id
                    format TSVWithNames"""
        df_cube_2 = ch_get_df(query=query)
        return df_cube_2
    
    @task()
    def transfrom_join(df_cube_1, df_cube_2):
        df_cube_join = df_cube_1.set_index('user_id').join(df_cube_2.set_index('user_id'), how = 'inner', rsuffix='_left')
        return df_cube_join
    
    @task()
    def transfrom_os(df_cube_join):
        df_cube_os = df_cube_join.groupby(['date', 'os']).sum().reset_index()
        df_cube_os = df_cube_os[['date', 'os', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_os = pd.melt(df_cube_os, id_vars=['date', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'], var_name='dimension', value_name='dimension_value')[['date','dimension', 'dimension_value', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return df_os
    
    @task()
    def transfrom_gen(df_cube_join):
        df_cube_gen = df_cube_join.groupby(['date', 'gender']).sum().reset_index()
        df_cube_gen = df_cube_gen[['date', 'gender', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_gen = pd.melt(df_cube_gen, id_vars=['date', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'], var_name='dimension', value_name='dimension_value')[['date','dimension', 'dimension_value', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return df_gen
    
    @task()
    def transfrom_age(df_cube_join):
        df_cube_age = df_cube_join.groupby(['date', 'age']).sum().reset_index()
        df_cube_age = df_cube_age[['date', 'age', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_age = pd.melt(df_cube_age, id_vars=['date', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent'], var_name='dimension', value_name='dimension_value')[['date','dimension', 'dimension_value', 'likes', 'views', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        return df_age
    
    @task()
    def make_total_table(df_os, df_gen, df_age):
        result = pd.concat([df_os, df_gen, df_age], sort=False, axis=0, ignore_index=True)
        print(result.to_csv(index=False, sep='\t'))
        return result
      
    @task()
    def load(df_total):
        context = get_current_context()
        ds = context['ds']
        print(f'Result load {ds}')
        ch_add_df(df_total, query_create)
        
    df_cube_1 = extract_1()
    df_cube_2 = extract_2()
    df_cube_join = transfrom_join(df_cube_1, df_cube_2)
    df_os = transfrom_os(df_cube_join)
    df_gen = transfrom_gen(df_cube_join)
    df_age = transfrom_age(df_cube_join)
    df_total = make_total_table(df_os, df_gen, df_age)
    load(df_total)

dag_task_6_1 = dag_task_6_1()
