import os

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import StructType

from consts.consts import (GH_FILE_URL_TEMPLATE,
                           POSTGRES_JDBC_PATH,
                           SPARK_APP_NAME, Counts, DB_Tables, Event, User, DATA_HOST)
from spark.util.db_connector import db_read, db_upsert, db_write
from spark.util.udfs import rep_own_incr_udf, commits_incr_udf

if __name__ == "__main__":
    cur_date = "2022-10-10"  # TODO: should be parametrized
    spark = (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config("spark.jars", POSTGRES_JDBC_PATH)
        .getOrCreate()
    )
    df_aggr_daily = spark.createDataFrame([], StructType([]))
    # iterate through hours
    for hour in range(24):
        date_hour = f"{cur_date}-{hour}"
        file_url = GH_FILE_URL_TEMPLATE.format(date_hour=date_hour, data_host=DATA_HOST)
        spark.sparkContext.addFile(file_url)
        df = spark.read.json(SparkFiles.get(os.path.basename(file_url)))

        # Create indicator columns for events to count
        df = df.withColumns(
            {
                Counts.REP_OWNERSHIP: rep_own_incr_udf(df[Event.TYPE]),
                Counts.COMMITS: commits_incr_udf(df[Event.TYPE], df[Event.PAYLOAD]),
            }
        )

        #Count commits and new repo ownerships
        df_aggr_hour = df.groupby(Event.ACTOR_ID).agg(
            spark_max(Event.ACTOR_LOGIN).alias(User.LOGIN),
            spark_sum(Counts.REP_OWNERSHIP).alias(Counts.REP_OWNERSHIP),
            spark_sum(Counts.COMMITS).alias(Counts.COMMITS),
        )

        df_aggr_daily = df_aggr_daily.unionByName(
            df_aggr_hour, allowMissingColumns=True
        )

    #Aggregate daily counts
    df_aggr_daily = df_aggr_daily.groupby(User.ID).agg(
        spark_max(User.LOGIN).alias(User.LOGIN),
        spark_sum(Counts.REP_OWNERSHIP).alias(Counts.REP_OWNERSHIP),
        spark_sum(Counts.COMMITS).alias(Counts.COMMITS),
    )

    # First update daily commits table
    db_write(df_aggr_daily.orderBy(col(Counts.COMMITS).desc()), DB_Tables.COMMITS)

    # Get existing users from db
    existing_users = db_read(spark, DB_Tables.USERS)
    all_users = existing_users.unionByName(df_aggr_daily)
    all_users.groupby(User.ID).agg(
        spark_max(User.LOGIN).alias(User.LOGIN),
        spark_sum(Counts.REP_OWNERSHIP).alias(Counts.REP_OWNERSHIP),
    )
    # TODO: enrich table with user properties
    db_upsert(DB_Tables.USERS, all_users, User.ID)
