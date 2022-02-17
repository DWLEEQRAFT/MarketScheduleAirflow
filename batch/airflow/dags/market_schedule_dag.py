from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

from datetime import timedelta
from datetime import datetime
import traceback
import pendulum
#import shutil
import time
import os

import exchange_calendars as xcals
import csv
import pandas as pd
from pandas import DataFrame
import boto3

## 휴일자 데이터 라이브러리
#import pandas_market_calendars as mcal


__TASK__ = 'market_schedule_download'
KST = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'retries':3,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": send_message
}

with DAG(
    __TASK__,
    default_args=default_args,
    dag_id='market_schedule',
    description=__TASK__,
    schedule_interval="@daily",
    start_date=datetime(2022, 2, 20, tzinfo=KST), ## 업로드 일정에 맞춰서 수정
    tags=[__TASK__],
) as dag:

    total_process = []

    def initial_market_schedule (exchange_symbol):
        try :
            mktCalander = xcals.get_calendar(
                exchange_symbol,
                side="left")
            dfMktCalander = mktCalander.schedule
            dfMktCalander.to_csv(os.path.curdir + '\\'+ exchange_symbol +'_market_schedule.csv', sep=',', na_rep='NaN')
        except Exception as e :
            print(f"error occured in intial data downloading")

        return dfMktCalander

    def update_market_schedule(exchange_symbol) :
        try :
            today = datetime.today.strftime('%Y-%m-%d')
            mktCalander = xcals.get_calendar(
                exchange_symbol,
                start = today,
                side = "left")
            dfMktCalander = mktCalander.schedule
            dfMktCalander.to_csv(os.path.curdir + '\\'+ exchange_symbol +'_market_schedule.csv', sep=',', na_rep='NaN')
        except Exception as e :
            print(f"error occured in update data downloading")

    ### 휴일 데이터 description 미 존재 및, 한국거래소 휴장일 데이터 소스 관련 문제로 HOLD
    '''
    def update_market_holiday(exchange_symbol) :
        try :
            if exchange_symbol == 'US' :
                calendar_holi = mcal.get_calendar('NYSE')
            elif exchange_symbol == 'KR' :
                ##create hard coded holiday description
                mktKRCalander = xcals.get_calendar(
                    xkrx,
                    start=today,
                    side="left")
            else :
                print(f"undefined exchanged market symbol is entered")

            dfMktCalander = mktCalander.schedule
            dfMktCalander.to_csv(os.path.curdir + '\\'+ exchange_symbol +'_market_holiday.csv', sep=',', na_rep='NaN')
        except Exception as e :
            print(f"error occured in update data downloading")
    '''


    def update_DB(exchange_symbol, dynamodb=None):

        df = pd.read_csv(os.path.curdir + '\\'+ exchange_symbol +'_market_schedule.csv', index_col = 0)

        if not dynamodb:
            ### 성민님 DB 세팅 되는대로 변경
            ### df 상에는 numpy.datetime64 타입으로 저장되어있으나, db 상에는 
            dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

        table = dynamodb.Table('market_schedule')

        for index, row in df.iterrows() :
            exMarket = row["exchange_symbol"]
            date = row["date"]
            response = table.update_item(
                Key={
                    'exchange_symbol': exMarket,
                    'date': date
                },
                UpdateExpression="set market_open=:mo, break_start=:bs, break_end=:be, market_close = :mc",
                ExpressionAttributeValues={
                    ':mo': row["market_open"],
                    ':bs': row["break_start"],
                    ':be': row["break_end"],
                    ':mc': row["market_close"]
                },
                ReturnValues="UPDATED_NEW"
            )

        return response

    total_process.append(
        PythonOperator(
            task_id=f"update-KR-market-schedule",
            python_callable=update_market_data,
            op_kwargs={"exchange_symbol": "XKRX"}
        )
    )
    total_process.append(
        PythonOperator(
            task_id=f"update-US-market-schedule",
            python_callable=update_market_data,
            op_kwargs={"exchange_symbol": "XNYS"}
        )
    )
    total_process.append(
        PythonOperator(
            task_id=f"update-DB-KR",
            python_callable=update_DB,
            op_kwargs={
                "exchange_symbol": "XKRX"
            }
        )
    )
    total_process.append(
        PythonOperator(
            task_id=f"update-DB-US",
            python_callable=update_DB,
            op_kwargs={"exchange_symbol": "XNYS"}
        )
    )

    for n in range(1, len(total_process)):
        total_process[n-1].set_downstream(total_process[n])