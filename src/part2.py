import sys

import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, DecimalType
import pandas as pd

# Выходные данные

presentation_clients: StructType = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Customer_Average_Check", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Customer_Average_Check_Segment", dataType=StringType(), nullable=True),
    StructField(name="Customer_Frequency", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Customer_Frequency_Segment", dataType=StringType(), nullable=True),
    StructField(name="Customer_Inactive_Period", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Customer_Churn_Rate", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Customer_Churn_Segment", dataType=StringType(), nullable=True),
    StructField(name="Customer_Segment", dataType=IntegerType(), nullable=True),
    StructField(name="Customer_Primary_Store", dataType=StringType(), nullable=True)
])

presentation_purchase_history = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Transaction_ID", dataType=StringType(), nullable=True),
    StructField(name="Transaction_DateTime", dataType=TimestampType(), nullable=True),
    StructField(name="Group_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_Cost", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Summ", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Summ_Paid", dataType=DecimalType(10, 2), nullable=True),
])

presentation_periods = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_ID", dataType=StringType(), nullable=True),
    StructField(name="First_Group_Purchase_Date", dataType=TimestampType(), nullable=True),
    StructField(name="Last_Group_Purchase_Date", dataType=TimestampType(), nullable=True),
    StructField(name="Group_Purchase", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Frequency", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Min_Discount", dataType=DecimalType(10, 2), nullable=True),
])

presentation_group = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_Affinity_Index", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Churn_Rate", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Stability_Index", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Margin", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Discount_Share", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Minimum_Discount", dataType=DecimalType(10, 2), nullable=True),
    StructField(name="Group_Average_Discount", dataType=DecimalType(10, 2), nullable=True),
])


