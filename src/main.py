from part1 import (schema_personal_data, schema_cards_table, schema_outlets, schema_transactions, schema_checks,
                   schema_SKU_group, schema_product_matrix, schema_date_of_analysis_generation)
from part2 import presentation_clients, presentation_group, presentation_periods, presentation_purchase_history
from part3 import check_user_status

import pyspark.sql
from pyspark.sql.functions import lit, col, when
from pyspark.sql import SparkSession
import pandas as pd
import os
from config import menu_info, Colors

from typing import Dict, List, Any


class Application:
    def __init__(self):  # инициализация класса
        self.spark: SparkSession = SparkSession.builder.appName("Application").getOrCreate()

        self.personal_data_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/personal_data.csv",
                                                                           header=True, schema=schema_personal_data)
        self.cards_table_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/cards_table.csv",
                                                                         header=True, schema=schema_cards_table)
        self.transactions_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/transactions_data.csv",
                                                                          header=True, schema=schema_transactions)
        self.checks_data_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/checks_data.csv",
                                                                         header=True, schema=schema_checks)
        self.product_matrix_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/product_matrix.csv",
                                                                            header=True, schema=schema_product_matrix)
        self.outlets_data_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/outlets_data.csv",
                                                                          header=True, schema=schema_outlets)
        self.sku_group_df: pyspark.sql.DataFrame = self.spark.read.csv(path="datasets/sku_groups.csv",
                                                                       header=True, schema=schema_SKU_group)
        self.analysis_date_df: pyspark.sql.DataFrame = self.spark.read.csv(
            path="datasets/date_of_analysis_generation_data.csv",
            header=True, schema=schema_date_of_analysis_generation)

        self.current_terminal_level: int = 0
        self.current_command: str = ""
        os.environ['USER_ROLE'] = "ADMIN"  # установка роли пользователя

        self.entity_map: Dict[str, pyspark.sql.DataFrame] = {
            "personal_data_df": self.personal_data_df,
            "cards_table_df": self.cards_table_df,
            "transactions_df": self.transactions_df,
            "checks_data_df": self.checks_data_df,
            "product_matrix_df": self.product_matrix_df,
            "outlets_data_df": self.outlets_data_df,
            "sku_group_df": self.sku_group_df,
            "analysis_date_df": self.analysis_date_df
        }

    def withdraw_card(self):  # отрисовка возможных действий
        print("\n\n\n")
        if self.current_terminal_level == 0:
            print(menu_info["root_menu"])

        elif self.current_terminal_level == 1:
            print(menu_info["first_level"])
            self.print_entities()

        elif self.current_terminal_level == 2:
            print(menu_info["second_level"])
            self.print_entities()

        elif self.current_terminal_level == 3:
            print(menu_info["third_level"])
            self.print_entities()

        elif self.current_terminal_level == 4:
            print(menu_info["forth_level"])
            self.print_entities()

        print(f"\nYou're status: {check_user_status()}. Current level is {self.current_terminal_level}. To exit to "
              f"the main menu, type 'b'. to quite program, type 'q'")

    def show_entity(self, entity_name: str, show_type: str, rows_amount: int) -> None:  # выводит сущность. Операция Read
        if (rows_amount < 0 or entity_name not in self.entity_map or
                self.entity_map[entity_name].count() < rows_amount or show_type not in ["head", "tail"]):

            print(Colors.RED + menu_info["enter_incorrect_values"] + Colors.RESET)
            return

        if show_type == "head":
            print(Colors.GREEN + '\n'.join([f'{row}' for row in self.entity_map[entity_name].head(rows_amount)]) + Colors.RESET)
            print(Colors.CYAN + menu_info["success"] + Colors.RESET)
            return

        print(Colors.GREEN + '\n'.join([f'{row}' for row in self.entity_map[entity_name].tail(rows_amount)]) + Colors.RESET)
        print(Colors.CYAN + menu_info["success"] + Colors.RESET)

    def print_entities(self) -> None:  # выводит сущности, доступные для взаимодействия
        print(', '.join([name for name in self.entity_map.keys()]))

    def change_entity_row(self, entity_name: str, row_id: str, attributes: str) -> None:  # изменяет запись в сущности. Операция Update
        attrs: List[str] = "".join(attributes).split(",")

        if (len(attrs) != len(self.entity_map[entity_name].columns) or entity_name not in self.entity_map or
                self.entity_map[entity_name].filter(row_id == self.entity_map[entity_name][self.entity_map[entity_name].columns[0]]).count() == 0):
            print(Colors.RED + menu_info["not_amount_attrs"] + Colors.RESET)
            return

        id_column: str = self.entity_map[entity_name].columns[0]
        for column, val in (zip(self.entity_map[entity_name].columns, attrs)):
            self.entity_map[entity_name] = self.entity_map[entity_name].withColumn(column, when(
                row_id == col(id_column), lit(val)).otherwise(col(column)))

        print(Colors.CYAN + menu_info["success"] + Colors.RESET)

    def add_entity_row(self, entity_name: str, attributes: str):  # добавляет запись в сущность. Операция Create
        new_row_data: List[Any] = attributes.split(",")
        total_columns: int = len(self.entity_map[entity_name].columns)
        if len(new_row_data) != total_columns:
            print(Colors.RED + menu_info["not_amount_attrs"] + Colors.RESET)
            return

        new_row_df = self.spark.createDataFrame([new_row_data], self.entity_map[entity_name].columns)
        self.entity_map[entity_name] = self.entity_map[entity_name].union(new_row_df)
        print(Colors.CYAN + menu_info["success"] + Colors.RESET)

    def delete_entity_row(self, entity_name: str, row_to_delete: str):  # удаляет запись из сущности. Операция Delete
        if (entity_name not in self.entity_map or
                self.entity_map[entity_name].filter(row_to_delete == self.entity_map[entity_name][self.entity_map[entity_name].columns[0]]).count() == 0):
            print(Colors.RED + menu_info["not_amount_attrs"] + Colors.RESET)
            return

        id_col: str = self.entity_map[entity_name].columns[0]

        self.entity_map[entity_name] = self.entity_map[entity_name].filter(self.entity_map[entity_name][id_col] != row_to_delete)
        print(Colors.CYAN + menu_info["success"] + Colors.RESET)

    def get_command(self) -> None:  # по сути главный метод, который отвечает за маршрутизацию
        # посмотреть номера уровней можно при запуске программы
        self.current_command = input()

        if self.current_command == "b":
            self.current_terminal_level = 0
            return

        if self.current_terminal_level == 0:
            if self.current_command not in ["1", "2", "3", "4", "q"]:
                print(Colors.RED + menu_info["no_such_command"] + Colors.RESET)

            else:
                self.current_terminal_level = int(self.current_command)

            return

        elif self.current_terminal_level == 1:
            entity_read_type: List[str] = self.current_command.split("-")
            if len(entity_read_type) != 3:
                print(Colors.RED + menu_info["enter_incorrect_values"] + Colors.RESET)
                return

            self.show_entity(entity_name=entity_read_type[0], show_type=entity_read_type[1],
                             rows_amount=int(entity_read_type[-1]))

        else:
            if not check_user_status():
                print(Colors.RED + menu_info["not_enough_roots"] + Colors.RESET)
                return

        if self.current_terminal_level == 2:
            new_values: List[str] = self.current_command.split("-")
            if len(new_values) != 3:
                print(Colors.RED + menu_info["enter_incorrect_values"] + Colors.RESET)
                return

            self.change_entity_row(entity_name=new_values[0], row_id=new_values[1], attributes=new_values[-1])

        elif self.current_terminal_level == 3:
            new_values: List[str] = self.current_command.split("-")
            if len(new_values) != 2:
                print(Colors.RED + menu_info["enter_incorrect_values"] + Colors.RESET)
                return

            self.add_entity_row(entity_name=new_values[0], attributes=new_values[-1])

        elif self.current_terminal_level == 4:
            delete_row_entity: List[str] = self.current_command.split("-")
            if len(delete_row_entity) != 2:
                print(Colors.RED + menu_info["enter_incorrect_values"] + Colors.RESET)
                return

            self.delete_entity_row(entity_name=delete_row_entity[0], row_to_delete=delete_row_entity[-1])

    def run(self):  # бесконечный цикл отрисовки и получения команды
        while self.current_command.lower() != "q":
            self.withdraw_card()
            self.get_command()


if __name__ == "__main__":
    app: Application = Application()
    app.run()
