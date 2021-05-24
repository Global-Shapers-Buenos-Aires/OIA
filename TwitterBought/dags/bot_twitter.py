from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from consts import API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET
import tweepy
from random import randint


# Authenticate to Twitter
auth = tweepy.OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

# Create API object
api = tweepy.API(auth)


# TASKS

def _scrap_data():
    # SCRAP DATA FROM WEB COMPR.AR

    # RETURN THE DATA OBJECT
    return randint(1, 10)


def _format_data(ti):
    # FETCH RAW DATA FROM WEB SCRAPPING TASK
    rawdata = ti.xcom_pull(task_ids='scrap_data')

    # PRE PROCESS DATA
    data_processed = rawdata*2

    # RETURN DATA FORMATTED
    return data_processed


def _publish_tweet(ti):

    # FETCH FORMATTED DATA FROM FORMAT DATA TASK
    data = ti.xcom_pull(task_ids='format_data')

    # CREATE AND PUBLISH TWEET OBJECT WITH IT
    return api.update_status(f"Data point: {data}")


# first time triggered at start_date + schedule_interval
with DAG("bot_twitter", 
start_date=datetime(2021, 5, 22),
schedule_interval="@daily", 
catchup=False) as dag:

    scrap_data = PythonOperator(
        task_id="scrap_data",
        python_callable=_scrap_data

    )

    format_data = PythonOperator(
        task_id="format_data",
        python_callable=_format_data

    )    

    publish_tweet = PythonOperator(
        task_id="publish_tweet",
        python_callable=_publish_tweet

    )

scrap_data >> format_data >> publish_tweet