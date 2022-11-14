from pyspark.sql import DataFrame, SparkSession

from consts.consts import DB_Creds


def db_read(spark_session: SparkSession, source_table: str) -> DataFrame:
    return (
        spark_session.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{DB_Creds.HOST}:{DB_Creds.PORT}/{DB_Creds.NAME}")
        .option("dbtable", source_table)
        .option("user", DB_Creds.USER)
        .option("password", DB_Creds.PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def db_write(data_frame: DataFrame, target_table: str):
    data_frame.write.format("jdbc").option(
        "url", f"jdbc:postgresql://{DB_Creds.HOST}:{DB_Creds.PORT}/{DB_Creds.NAME}")\
        .option("dbtable", target_table)\
        .option("user", DB_Creds.USER)\
        .option("password", DB_Creds.PASSWORD
    ).option(
        "driver", "org.postgresql.Driver"
    ).save()


def db_upsert(target_table: str, data_frame: DataFrame, index_column: str):
    #TODO: db_upsert
    pass
