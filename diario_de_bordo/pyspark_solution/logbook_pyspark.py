from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, count, max, min, avg, when

def create_spark_session():
    spark = SparkSession.builder \
        .appName("Logbook") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark


def convert_to_date_format(dataframe: DataFrame, column_date: str) -> DataFrame:
    dataframe = dataframe.withColumn(column_date, to_date(col(column_date), 'MM-dd-yyyy HH:mm'))
    return dataframe

def create_normalized_date_column(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.withColumn("DT_REFE", col("DATA_INICIO").cast("date"))
    return dataframe

def aggregate_logbook_by_date(dataframe: DataFrame) -> DataFrame:
    dataframe_agg = dataframe.groupBy("DT_REFE").agg(
        count("DT_REFE").alias("QT_CORR"),
        count(when(col("CATEGORIA") == "Negocio", True)).alias("QT_CORR_NEG"),
        count(when(col("CATEGORIA") == "Pessoal", True)).alias("QT_CORR_PESS"),
        max("DISTANCIA").alias("VL_MAX_DIST"),
        min("DISTANCIA").alias("VL_MIN_DIST"),
        avg("DISTANCIA").alias("VL_AVG_DIST"),
        count(when(col("PROPOSITO") == "Reunião", True)).alias("QT_CORR_REUNI"),
        count(when((col("PROPOSITO") != "Reunião") | col("PROPOSITO").isNull(), True)).alias("QT_CORR_NAO_REUNI")
    )
    dataframe_agg = dataframe_agg.withColumn("VL_AVG_DIST", col("VL_AVG_DIST").cast("decimal(5,2)"))
    dataframe_agg = dataframe_agg.orderBy("DT_REFE")
    return dataframe_agg

def extract(path: str, spark: SparkSession) -> DataFrame:
    racing_info = spark.read.csv(path, sep=";", header=True, inferSchema=True)
    print("Data extracted successfully.")
    return racing_info

def transform(dataframe: DataFrame) -> DataFrame:
    df_transformed = convert_to_date_format(dataframe, "DATA_INICIO")
    df_transformed = create_normalized_date_column(df_transformed)
    df_agg = aggregate_logbook_by_date(df_transformed)
    print("Data transformed successfully.")
    return df_agg

def load(dataframe: DataFrame, path: str):
    dataframe.coalesce(1).write.parquet(f'{path}/parquet', mode='overwrite')
    dataframe.coalesce(1).write.csv(f'{path}/csv', header=True, sep=";", mode='overwrite')
    print("Data saved successfully.")

def main():
    spark = create_spark_session()
    racing_info = extract("./diario_de_bordo/info_transportes.csv", spark)
    logbook = transform(racing_info)
    load(logbook, "./diario_de_bordo/results")
    spark.stop()

if __name__ == "__main__":
    main()
