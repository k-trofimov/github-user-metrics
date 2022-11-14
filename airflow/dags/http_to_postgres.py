import gzip
import json
import os
import sys
import tempfile

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from sqlalchemy_utils.types.enriched_datetime.pendulum_datetime import pendulum


@dag(
    schedule="0 8 * * *", # TODO replace with variable
    start_date=pendulum.datetime(2022, 10, 9, tz="UTC"), # TODO replace with variable
    catchup=False
)
def http_to_PG():
    """
    This is a simple pipeline to extract data from hithub archive api and load it into Postgres database
    """
    @task
    def spark_submit():
        return SparkSubmitOperator(
        application="${SPARK_HOME}/pull_aggregate.py",
        task_id='http_to_PG',
        conn_id='spark_default')

    spark_submit()


http_to_PG()