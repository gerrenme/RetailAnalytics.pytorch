import pyspark
import pyspark.sql.functions as F

import pandas as pd
from dataframes import DataClass

from typing import List, Tuple
from datetime import datetime


class Repository:
    def __init__(self):
        self.dataframes: DataClass = DataClass()

    def print_entities(self):
        print("\t" + ", ".join([name for name in self.dataframes.entity_map.keys()]))

    def read_entity_rows(self, entity_name: str, rows: int, read_type: str = "head") -> str:
        res: str = ""
        if read_type == "head":
            res = "\n".join([f"{row}" for row in self.dataframes.entity_map[entity_name].head(rows)])

        elif read_type == "tail":
            res = "\n".join([f"{row}" for row in self.dataframes.entity_map[entity_name].tail(rows)])

        return res

    def change_entity_row(self, entity_name: str, attributes: List[str], row_id: str) -> None:
        id_column: str = self.dataframes.entity_map[entity_name].columns[0]

        for column, val in (zip(self.dataframes.entity_map[entity_name].columns[1:], attributes)):
            self.dataframes.entity_map[entity_name] = self.dataframes.entity_map[entity_name].withColumn(column, F.when(
                row_id == F.col(id_column), F.lit(val)).otherwise(F.col(column)))

        return

    def add_entity_row(self, entity_name: str, new_row_data: List[str]) -> None:
        new_row_df = self.dataframes.spark.createDataFrame([new_row_data],
                                                           self.dataframes.entity_map[entity_name].columns)
        self.dataframes.entity_map[entity_name] = self.dataframes.entity_map[entity_name].union(new_row_df)

        return

    def delete_entity_row(self, entity_name: str, row_to_delete: str) -> None:
        id_col: str = self.dataframes.entity_map[entity_name].columns[0]

        self.dataframes.entity_map[entity_name] = self.dataframes.entity_map[entity_name].filter(
            row_to_delete != self.dataframes.entity_map[entity_name][id_col])

        return

    def save_new_entity(self, entity_name: str, filepath: str) -> None:
        pd_df: pd.DataFrame = self.dataframes.entity_map[entity_name].toPandas()
        pd_df.to_csv(filepath, index=False)

        return

    def get_grouped_data(self, max_churn_index: float, max_share_transactions: float) -> pyspark.sql.DataFrame:
        filtered_groups: pyspark.sql.DataFrame = self.dataframes.groups_presentation_df.filter(
            (F.col("Group_Churn_Rate") <= max_churn_index) &
            (F.col("Group_Discount_Share") < max_share_transactions))

        filtered_groups = filtered_groups.orderBy(
            F.desc("Group_Affinity_Index"),
            F.desc("Group_Churn_Rate"),
            F.desc("Group_Discount_Share")
        )

        grouped_data = filtered_groups.groupBy("Customer_ID").agg(
            F.collect_list("Group_ID")[0].alias("Group_Name")
        )

        return grouped_data

    def get_min_max_dates(self) -> Tuple[datetime, datetime]:
        min_date: str = self.dataframes.transactions_df.select(F.min("Transaction_DateTime")).collect()[0][0]
        min_date_dt: datetime = datetime.strptime(min_date, "%Y-%m-%d %H:%M:%S")

        max_date: str = self.dataframes.transactions_df.select(F.max("Transaction_DateTime")).collect()[0][0]
        max_date_dt: datetime = datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")

        return min_date_dt, max_date_dt

    def get_max_sku_diff(self) -> str:
        res: str = self.dataframes.outlets_data_df.select(
            F.max(F.col("SKU_RetailPrice") - F.col("SKU_Purchase_Price")).alias("max_diff")
        ).first()["max_diff"]

        return res

    def get_max_sku_id(self, max_diff_val: float) -> str:
        res: str =self.dataframes.outlets_data_df.where(
            F.col("SKU_RetailPrice") - F.col("SKU_Purchase_Price") == max_diff_val
        ).select(F.col("SKU_ID")).first()["SKU_ID"]

        return res

    def get_sku_id_amount(self, sku_id: str) -> Tuple[str, str]:
        res: Tuple[str, str] = self.dataframes.outlets_data_df.select(
            F.count(F.col("SKU_ID")).alias("Total_Count"),
            F.sum((F.col("SKU_ID") == sku_id).cast("int")).alias("Count_of_MAX_SKU")
        ).first()

        return res

    def get_sku_prices(self, sku_id: str) -> pyspark.sql.Row:
        res: pyspark.sql.Row = self.dataframes.outlets_data_df.filter(F.col("SKU_ID") == sku_id).select(
            F.col("SKU_RetailPrice").alias("Retail_Price"),
            F.col("SKU_Purchase_Price").alias("Purchase_Price"))

        return res

    def create_client_groups(self, group_amount: int) -> pyspark.sql.DataFrame:
        groups_slice: F.expr = F.expr(f"slice(Customer_Groups, 1, {group_amount}) as Client_Groups")

        client_groups: pyspark.sql.DataFrame = self.dataframes.personal_data_df.join(self.dataframes.cards_table_df,
                                                                                     on="Customer_ID", how="inner")
        client_groups = client_groups.join(self.dataframes.transactions_df, on="Customer_Card_ID", how="inner")
        client_groups = client_groups.join(self.dataframes.checks_data_df, on="Transaction_ID", how="inner")
        client_groups = client_groups.select(["Customer_ID", "SKU_ID"]).join(self.dataframes.product_matrix_df,
                                                                             on="SKU_ID", how="inner")
        client_groups = client_groups.select(["Customer_ID", "SKU_ID", "Group_ID"])
        client_groups = (client_groups.groupBy("Customer_ID").agg(F.collect_set("Group_ID").alias("Customer_Groups")).
                         select("Customer_ID", groups_slice))

        return client_groups

    def create_good_groups(self, max_churn_ind: float, max_stab_ind: float) -> pyspark.sql.DataFrame:
        res: pyspark.sql.DataFrame = self.dataframes.groups_presentation_df.filter(
            (F.col("Group_Churn_Rate") <= max_churn_ind) &
            (F.col("Group_Stability_Index") < max_stab_ind)).orderBy(
            F.desc("Group_Affinity_Index")).groupBy("Customer_ID").agg(
            F.first("Group_ID").alias("Group_ID")
        )

        return res

    def get_limit_transactions(self, limit: int) -> pyspark.sql.DataFrame:
        res: pyspark.sql.DataFrame = self.dataframes.transactions_df.orderBy(
            F.col("Transaction_DateTime").desc()).limit(limit)

        return res

    def get_last_transactions(self, first_date: datetime, last_date: datetime) -> pyspark.sql.DataFrame:
        res: pyspark.sql.DataFrame = self.dataframes.transactions_df.filter(
                (F.col("Transaction_DateTime") >= first_date) &
                (F.col("Transaction_DateTime") <= last_date))

        return res

    def group_transactions_cards(self, last_transactions: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        res: pyspark.sql.DataFrame = last_transactions.join(
            self.dataframes.cards_table_df, on="Customer_Card_ID", how="inner")

        return res

    def create_min_discount(self) -> pyspark.sql.DataFrame:
        min_discount: pyspark.sql.DataFrame = self.dataframes.groups_presentation_df.groupBy("Group_ID").agg(
            F.min("Group_Minimum_Discount").alias("Min_Group_Discount"))
        # noinspection PyTypeChecker
        min_discount = min_discount.withColumn("Min_Group_Discount", F.col("Min_Group_Discount") + 0.05)

        return min_discount

    def create_between_date(self, first_date: datetime, last_date: datetime,
                            date_dif: int, add_transactions: int) -> pyspark.sql.DataFrame:
        frequency: pyspark.sql.DataFrame = self.dataframes.clients_presentation_df.select("Customer_ID",
                                                                                          "Customer_Frequency")
        between_date: pyspark.sql.DataFrame = self.dataframes.transactions_df.filter(
            (F.col("Transaction_DateTime") >= self.get_min_max_dates()[0])
            & (F.col("Transaction_DateTime") <= self.get_min_max_dates()[1]))

        between_date = between_date.withColumn("Start_Date", F.lit(first_date.date()))
        between_date = between_date.withColumn("End_Date", F.lit(last_date.date()))
        between_date = between_date.join(self.dataframes.cards_table_df, on="Customer_Card_ID", how="inner")
        between_date = between_date.join(frequency, on="Customer_ID", how="inner")
        # noinspection PyTypeChecker
        between_date = between_date.withColumn("Required_Transactions_Count",
                                               F.round(date_dif / F.col("Customer_Frequency")) + add_transactions)

        return between_date

    def get_final_margin(self, max_margin_call: float) -> pyspark.sql.DataFrame:
        average_margin: pyspark.sql.DataFrame = (
            self.dataframes.purchase_presentation_df.groupBy("Group_ID").agg(
                F.avg("Group_Summ").alias("Average_Check")))

        # noinspection PyTypeChecker
        average_margin = average_margin.withColumn("Average_Check_Margin",
                                                   F.col("Average_Check") * max_margin_call)

        min_discount: pyspark.sql.DataFrame = self.dataframes.groups_presentation_df.groupBy("Group_ID").agg(
            F.min("Group_Minimum_Discount").alias("Min_Group_Discount"))
        # noinspection PyTypeChecker
        min_discount = min_discount.withColumn("Min_Group_Discount", F.col("Min_Group_Discount") + 0.05)

        average_margin = average_margin.join(min_discount, on="Group_ID", how="inner")
        average_margin = average_margin.drop("Average_Check")

        return average_margin

    def cast_groups_data_float(self):
        self.dataframes.groups_presentation_df = self.dataframes.groups_presentation_df.withColumn(
            "Group_Churn_Rate", F.col("Group_Churn_Rate").cast("float"))
        self.dataframes.groups_presentation_df = self.dataframes.groups_presentation_df.withColumn(
            "Group_Discount_Share",
            F.col("Group_Discount_Share").cast("float"))

    @staticmethod
    def get_final_between_date(between_date: pyspark.sql.DataFrame, grouped_data: pyspark.sql.DataFrame,
                               min_discount: pyspark.sql.DataFrame, max_margin_call: float) -> pyspark.sql.DataFrame:
        between_date = between_date.join(grouped_data, on="Customer_ID", how="inner")
        between_date = between_date.join(min_discount, on=between_date["Group_Name"] == min_discount["Group_ID"],
                                         how="inner")
        between_date = between_date.withColumn("Offer_Discount_Depth",
                                               F.greatest(F.col("Min_Group_Discount"), F.lit(max_margin_call / 100)))

        between_date = between_date.drop("Transaction_ID", "Customer_Card_ID", "Transaction_DateTime",
                                         "Transaction_Store_ID", "Transaction_Summ", "Customer_Frequency", "Group_ID",
                                         "Min_Group_Discount")

        return between_date

    @staticmethod
    def get_turnover_sum(last_transactions: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        res: pyspark.sql.DataFrame = last_transactions.groupBy("Customer_ID").agg(
            F.sum("Transaction_Summ").alias("SumTurnover"),
            F.count("Transaction_ID").alias("TransactionCount"))

        return res

    @staticmethod
    def find_offer_disc_depth(dataframe: pyspark.sql.DataFrame,
                              margin_call_ratio: float) -> pyspark.sql.DataFrame:
        res = dataframe.withColumn(
            "Offer_Discount_Depth",
            F.when(F.col("Group_ID").isNull(), None).otherwise(margin_call_ratio + 0.05)
        )

        return res

    @staticmethod
    def find_user_groups(user_good_groups: pyspark.sql.DataFrame,
                         client_groups: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        res: pyspark.sql.DataFrame = user_good_groups.join(
            client_groups, on="Customer_ID", how="left_outer").withColumn(
            "Group_ID",
            F.expr("CASE WHEN array_contains(Client_Groups, Group_ID) THEN Group_ID ELSE NULL END")
        ).drop("Client_Groups")

        return res

    @staticmethod
    def get_final_average_check(average_check: pyspark.sql.DataFrame, grouped_data: pyspark.sql.DataFrame,
                                average_margin: pyspark.sql.DataFrame, max_margin_call: float) -> pyspark.sql.DataFrame:
        average_check = average_check.join(grouped_data, on="Customer_ID", how="inner")
        average_check = average_check.join(average_margin, on=average_margin["Group_ID"] == average_check["Group_Name"],
                                           how="inner")
        average_check = average_check.drop("TransactionCount", "SumTurnover", "Average_Check_Margin", "Group_ID")

        average_check = average_check.withColumn("Min_Group_Discount",
                                                 F.greatest(F.col("Min_Group_Discount"), F.lit(max_margin_call / 100)))

        average_check = average_check.orderBy(F.desc("Required_Check_Measure"))

        return average_check

    @staticmethod
    def create_average_check(sum_turnover: pyspark.sql.DataFrame, average_bill_inc: float) -> pyspark.sql.DataFrame:
        res: pyspark.sql.DataFrame = sum_turnover.withColumn(
            "Required_Check_Measure", F.col('SumTurnover') / F.col('TransactionCount') * average_bill_inc)

        return res
