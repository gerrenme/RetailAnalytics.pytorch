from dataclasses import dataclass

import pyspark.sql
from pyspark.sql import SparkSession

from part1 import (schema_personal_data, schema_cards_table, schema_outlets, schema_transactions, schema_checks,
                   schema_SKU_group, schema_product_matrix, schema_date_of_analysis_generation)
from part2 import presentation_clients, presentation_group, presentation_periods, presentation_purchase_history

from typing import Dict


@dataclass
class DataClass:
    def __init__(self):
        self.spark: SparkSession = SparkSession.builder.appName("Application").getOrCreate()

        self.personal_data_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/personal_data_data.csv",
                                                                           header=True, schema=schema_personal_data)
        self.cards_table_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/cards_table_data.csv",
                                                                         header=True, schema=schema_cards_table)
        self.transactions_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/transactions_data.csv",
                                                                          header=True, schema=schema_transactions)
        self.checks_data_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/checks_data_data.csv",
                                                                         header=True, schema=schema_checks)
        self.product_matrix_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/product_matrix_data.csv",
                                                                            header=True, schema=schema_product_matrix)
        self.outlets_data_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/outlets_data_data.csv",
                                                                          header=True, schema=schema_outlets)
        self.sku_group_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/sku_group_data.csv",
                                                                       header=True, schema=schema_SKU_group)
        self.date_of_analysis_generation_df: pyspark.sql.DataFrame = self.spark.read.csv(
            path="datasets/date_of_analysis_generation_data.csv",
            header=True, schema=schema_date_of_analysis_generation)

        self.clients_presentation_df: pyspark.sql.DataFrame = self.spark.read.csv(
            path="datasets/clients_presentation_data.csv", header=True, schema=presentation_clients)

        self.purchase_presentation_df: pyspark.sql.DataFrame = self.spark.read.csv(
            path="datasets/purchase_presentation_data.csv", header=True, schema=presentation_purchase_history)

        self.period_presentation_df: pyspark.sql.DataFrame = self.spark.read.csv(
            path="datasets/period_presentation_data.csv", header=True, schema=presentation_periods)

        self.groups_presentation_df: pyspark.sql.DataFrame = self.spark.read.csv(
            path="datasets/groups_presentation_data.csv", header=True, schema=presentation_group)

        self.entity_map: Dict[str, pyspark.sql.DataFrame] = {
            "personal_data_df": self.personal_data_df,
            "cards_table_df": self.cards_table_df,
            "transactions_df": self.transactions_df,
            "checks_data_df": self.checks_data_df,
            "product_matrix_df": self.product_matrix_df,
            "outlets_data_df": self.outlets_data_df,
            "sku_group_df": self.sku_group_df,
            "analysis_date_df": self.date_of_analysis_generation_df,
            "clients_presentation_df": self.clients_presentation_df,
            "purchase_presentation_df": self.purchase_presentation_df,
            "period_presentation_df": self.period_presentation_df,
            "groups_presentation_df": self.groups_presentation_df
        }
