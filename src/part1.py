import sys

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, DecimalType
import pandas as pd

# Входные данные
schema_personal_data: StructType = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=False),
    StructField(name="Customer_Name", dataType=StringType(), nullable=False),
    StructField(name="Customer_Surname", dataType=StringType(), nullable=False),
    StructField(name="Customer_Primary_Email", dataType=StringType(), nullable=False),
    StructField(name="Customer_Primary_Phone", dataType=StringType(), nullable=True)
])

schema_cards_table: StructType = StructType([
    StructField(name="Customer_Card_ID", dataType=StringType(), nullable=False),
    StructField(name="Customer_ID", dataType=StringType(), nullable=False)
])

schema_transactions: StructType = StructType([
    StructField(name="Transaction_ID", dataType=StringType(), nullable=False),
    StructField(name="Customer_Card_ID", dataType=StringType(), nullable=False),
    StructField(name="Transaction_Summ", dataType=FloatType(), nullable=False),
    StructField(name="Transaction_DateTime", dataType=TimestampType(), nullable=False),
    StructField(name="Transaction_Store_ID", dataType=IntegerType(), nullable=False)
])

schema_checks: StructType = StructType([
    StructField(name="Transaction_ID", dataType=StringType(), nullable=False),
    StructField(name="SKU_ID", dataType=IntegerType(), nullable=False),
    StructField(name="SKU_Amount", dataType=IntegerType(), nullable=False),
    StructField(name="SKU_Summ", dataType=FloatType(), nullable=False),
    StructField(name="SKU_Summ_Paid", dataType=FloatType(), nullable=False),
    StructField(name="SKU_Summ_Discount", dataType=FloatType(), nullable=False)
])

schema_product_matrix: StructType = StructType([
    StructField(name="SKU_ID", dataType=StringType(), nullable=False),
    StructField(name="SKU_Name", dataType=StringType(), nullable=False),
    StructField(name="Group_ID", dataType=StringType(), nullable=True)
])

schema_outlets: StructType = StructType([
    StructField(name="Transaction_Store_ID", dataType=StringType(), nullable=False),
    StructField(name="SKU_ID", dataType=StringType(), nullable=False),
    StructField(name="SKU_Purchase_Price", dataType=FloatType(), nullable=True),
    StructField(name="SKU_RetailPrice", dataType=FloatType(), nullable=True)
])

schema_SKU_group: StructType = StructType([
    StructField(name="Group_ID", dataType=StringType(), nullable=False),
    StructField(name="Group_Name", dataType=StringType(), nullable=True)
])

schema_date_of_analysis_generation: StructType = StructType([
    StructField(name="Analysis_Formation", dataType=TimestampType(), nullable=False)
])
