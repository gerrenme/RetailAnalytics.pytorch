from pyspark.sql.types import StructType, StructField, StringType

# Выходные данные

presentation_clients: StructType = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Customer_Average_Check", dataType=StringType(), nullable=True),
    StructField(name="Customer_Average_Check_Segment", dataType=StringType(), nullable=True),
    StructField(name="Customer_Frequency", dataType=StringType(), nullable=True),
    StructField(name="Customer_Frequency_Segment", dataType=StringType(), nullable=True),
    StructField(name="Customer_Inactive_Period", dataType=StringType(), nullable=True),
    StructField(name="Customer_Churn_Rate", dataType=StringType(), nullable=True),
    StructField(name="Customer_Churn_Segment", dataType=StringType(), nullable=True),
    StructField(name="Customer_Segment", dataType=StringType(), nullable=True),
    StructField(name="Customer_Primary_Store", dataType=StringType(), nullable=True)
])

presentation_purchase_history = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Transaction_ID", dataType=StringType(), nullable=True),
    StructField(name="Transaction_DateTime", dataType=StringType(), nullable=True),
    StructField(name="Group_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_Cost", dataType=StringType(), nullable=True),
    StructField(name="Group_Summ", dataType=StringType(), nullable=True),
    StructField(name="Group_Summ_Paid", dataType=StringType(), nullable=True),
])

presentation_periods = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_ID", dataType=StringType(), nullable=True),
    StructField(name="First_Group_Purchase_Date", dataType=StringType(), nullable=True),
    StructField(name="Last_Group_Purchase_Date", dataType=StringType(), nullable=True),
    StructField(name="Group_Purchase", dataType=StringType(), nullable=True),
    StructField(name="Group_Frequency", dataType=StringType(), nullable=True),
    StructField(name="Group_Min_Discount", dataType=StringType(), nullable=True),
])

presentation_group = StructType([
    StructField(name="Customer_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_ID", dataType=StringType(), nullable=True),
    StructField(name="Group_Affinity_Index", dataType=StringType(), nullable=True),
    StructField(name="Group_Churn_Rate", dataType=StringType(), nullable=True),
    StructField(name="Group_Stability_Index", dataType=StringType(), nullable=True),
    StructField(name="Group_Margin", dataType=StringType(), nullable=True),
    StructField(name="Group_Discount_Share", dataType=StringType(), nullable=True),
    StructField(name="Group_Minimum_Discount", dataType=StringType(), nullable=True),
    StructField(name="Group_Average_Discount", dataType=StringType(), nullable=True),
])


