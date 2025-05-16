import pytest
from diario_de_bordo.pyspark_solution import logbook_pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql import DataFrame
from datetime import date
from pyspark.sql.types import DateType
from pyspark.sql import Row


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("TestLogbook") \
        .master("local[*]") \
        .getOrCreate()
    return spark
    

def test_convert_to_date_format(spark):
    data = [("01-13-2023 10:00",), ("02-01-2023 11:00",), ("03-20-2023 12:00",)]
    schema = StructType([StructField("DATA_INICIO", StringType(), True)])
    df = spark.createDataFrame(data, schema)
    
    df = logbook_pyspark.convert_to_date_format(df, "DATA_INICIO")

    expected_data = [(date(2023, 1, 13),), (date(2023, 2, 1),), (date(2023, 3, 20),)]
    expected_schema = StructType([StructField("DATA_INICIO", DateType(), True)])
    expected_df = spark.createDataFrame(expected_data, expected_schema)
   
    assert df.schema["DATA_INICIO"].dataType == DateType()
    assert df.collect() == expected_df.collect()

def test_create_normalized_date_column(spark):
    data = [(date(2023, 1, 13),), (date(2023, 2, 1),), (date(2023, 3, 20),)]
    schema = StructType([StructField("DATA_INICIO", DateType(), True)])
    df = spark.createDataFrame(data, schema)

    df = logbook_pyspark.create_normalized_date_column(df)

    expected_data = [(date(2023, 1, 13),), (date(2023, 2, 1),), (date(2023, 3, 20),)]
    expected_schema = StructType([StructField("DT_REFE", DateType(), True)])
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    resultado = [row.DT_REFE for row in df.select("DT_REFE").collect()]
    esperado = [row.DT_REFE for row in expected_df.collect()]

    assert df.schema["DT_REFE"].dataType == DateType()
    assert resultado == esperado

def test_aggregate_logbook_by_date(spark):
    data = [
        (date(2023, 1, 13), "Negocio", 10, "Reunião"),
        (date(2023, 1, 13), "Pessoal", 20, "Outro"),
        (date(2023, 2, 1), "Negocio", 30, "Reunião"),
    ]
    schema = ["DT_REFE", "CATEGORIA", "DISTANCIA", "PROPOSITO"]
    df = spark.createDataFrame(data, schema)

    result_df = logbook_pyspark.aggregate_logbook_by_date(df)
    result = {row.DT_REFE: row.asDict() for row in result_df.collect()}

    expected = {
        date(2023, 1, 13): {
            "QT_CORR": 2,
            "QT_CORR_NEG": 1,
            "QT_CORR_PESS": 1,
            "VL_MAX_DIST": 20,
            "VL_MIN_DIST": 10,
            "VL_AVG_DIST": 15.0,
            "QT_CORR_REUNI": 1,
            "QT_CORR_NAO_REUNI": 1,
        },
        date(2023, 2, 1): {
            "QT_CORR": 1,
            "QT_CORR_NEG": 1,
            "QT_CORR_PESS": 0,
            "VL_MAX_DIST": 30,
            "VL_MIN_DIST": 30,
            "VL_AVG_DIST": 30.0,
            "QT_CORR_REUNI": 1,
            "QT_CORR_NAO_REUNI": 0,
        }
    }

    for dt, exp_values in expected.items():
        for key, val in exp_values.items():
            assert result[dt][key] == val