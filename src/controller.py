from config import Colors, menu_info
from service import Service
from static_service import StaticService
from types_validate import TypesValidate

import pyspark

from typing import List, Tuple
from datetime import datetime

from repository import Repository


class Controller:
    def __init__(self):
        self.service: Service = Service()
        self.repo: Repository = Repository()

    def print_entities(self, terminal_level: int):
        if terminal_level not in [0, 6, 7, 8]:
            self.repo.print_entities()

    def show_entity(self, entity_name: str) -> None:  # YAP
        print(menu_info["chose_read_parameters"])
        data: List[str] = input().split(",")
        if StaticService.check_se_atrs(atrs=data):
            StaticService.print_error_inc_values()
            return

        read_type: str = data[0]
        rows_amount: int = int(data[-1])
        if self.service.check_se_operations(rows=rows_amount, ent_name=entity_name, read_type=read_type):
            StaticService.print_error_inc_values()
            return

        print(self.repo.read_entity_rows(entity_name=entity_name, rows=rows_amount, read_type=read_type))
        StaticService.print_success()

    def change_entity_row(self, entity_name: str) -> None:
        print(menu_info["change_row_params"])

        new_values: List[str] = input().split("|")
        if StaticService.check_atrs_len(atrs=new_values, need_len=2):
            StaticService.print_error_inc_values()
            return

        row_id: str = new_values[0]
        attributes: List[str] = new_values[-1].split(",")

        if self.service.check_ce_atrs(atrs=attributes, ent_name=entity_name, row_id=row_id):
            StaticService.print_error_not_amount_attrs()
            return

        self.repo.change_entity_row(entity_name=entity_name, attributes=attributes, row_id=row_id)
        StaticService.print_success()

    def add_entity_row(self, entity_name: str) -> None:  # YAP
        if self.service.check_entity_in_map(ent_name=entity_name):
            StaticService.print_error_inc_values()
            return

        print(menu_info["add_new_row"])

        new_row_data: List[str] = input().split(",")
        if self.service.check_ae_atrs(atrs=new_row_data, ent_name=entity_name):
            StaticService.print_error_not_amount_attrs()
            return

        self.repo.add_entity_row(entity_name=entity_name, new_row_data=new_row_data)
        StaticService.print_success()

    def delete_entity_row(self, entity_name: str) -> None:  # YAP
        print(menu_info["delete_row"])
        row_to_delete: str = input()

        if self.service.check_de_atrs(row_id=row_to_delete, ent_name=entity_name):
            StaticService.print_error_not_amount_attrs()
            return

        self.repo.delete_entity_row(entity_name=entity_name, row_to_delete=row_to_delete)
        StaticService.print_success()

    def save_entity(self, entity_name: str) -> None:  # YAP
        if self.service.check_entity_in_map(ent_name=entity_name):
            StaticService.print_error_inc_values()
            return

        filepath: str = f"datasets/{entity_name[:-3]}_data.csv"
        print(f"The entity will be saved in the following path: {filepath}. If you want to continue, enter 'Yes'")
        if input().lower() == "yes":
            self.repo.save_new_entity(entity_name=entity_name, filepath=filepath)
            StaticService.print_success()

        else:
            print(Colors.RED + "You canceled the save operation" + Colors.RESET)

    def get_offer_incr_check(self, atrs: str) -> None:
        attributes: List[str] = atrs.split(",")
        calculation_method: str = attributes[0]

        first_date: datetime = datetime.strptime("2000-12-12 12:12:12", "%Y-%m-%d %H:%M:%S")
        last_date: datetime = datetime.strptime("2000-12-12 12:12:12", "%Y-%m-%d %H:%M:%S")
        total_transactions: int = 0

        if StaticService.check_oi_atrs(atrs=attributes):
            StaticService.print_error_inc_values()
            return

        method_balancer: int = 0
        if calculation_method == "1":
            if StaticService.check_validate_sum_oi_date(atrs=attributes, need_sum=2):
                StaticService.print_error_inc_values()
                return

            first_date: datetime = datetime.strptime(attributes[1], "%Y-%m-%d %H:%M:%S")
            last_date: datetime = datetime.strptime(attributes[2], "%Y-%m-%d %H:%M:%S")

            first_date, last_date = self.get_first_last_date(first_date=first_date, last_date=last_date)

        elif calculation_method == "2":
            if not TypesValidate.validate_int(attributes[1]) or int(attributes[1]) < 0:
                StaticService.print_error_inc_values()
                return

            total_transactions: int = int(attributes[1])
            method_balancer = 1

        else:
            StaticService.print_error_inc_values()
            return

        if StaticService.check_validate_sum_oi(atrs=attributes, need_sum=4, balancer=method_balancer):
            StaticService.print_error_inc_values()
            return

        average_bill_inc: float = float(attributes[3 - method_balancer])
        max_churn_index: float = float(attributes[4 - method_balancer])
        max_share_transactions: float = float(attributes[5 - method_balancer])
        max_margin_call: float = float(attributes[6 - method_balancer])

        if calculation_method == "1":
            last_transactions: pyspark.sql.DataFrame = self.repo.get_last_transactions(first_date=first_date,
                                                                                       last_date=last_date)
        else:
            last_transactions: pyspark.sql.DataFrame = self.repo.get_limit_transactions(limit=total_transactions)

        last_transactions: pyspark.sql.DataFrame = self.repo.group_transactions_cards(
            last_transactions=last_transactions)
        sum_turnover: pyspark.sql.DataFrame = self.repo.get_turnover_sum(last_transactions=last_transactions)

        average_check: pyspark.sql.DataFrame = self.repo.create_average_check(sum_turnover=sum_turnover,
                                                                              average_bill_inc=average_bill_inc)

        self.repo.cast_groups_data_float()
        grouped_data: pyspark.sql.DataFrame = self.repo.get_grouped_data(max_churn_index=max_churn_index,
                                                                         max_share_transactions=max_share_transactions)

        average_margin: pyspark.sql.DataFrame = self.repo.get_final_margin(max_margin_call=max_margin_call)
        average_check = self.repo.get_final_average_check(average_check=average_check, grouped_data=grouped_data,
                                                          average_margin=average_margin,
                                                          max_margin_call=max_margin_call)

        average_check.show()

    def get_offer_incr_freq(self, atrs: str):
        attributes: List[str] = atrs.split(",")
        if StaticService.check_atrs_len(atrs=attributes, need_len=6):
            StaticService.print_error_inc_values()
            return

        if StaticService.check_validate_sum_if(atrs=attributes, need_sum=6):
            StaticService.print_error_inc_values()
            return

        first_date: datetime = datetime.strptime(attributes[0], "%Y-%m-%d %H:%M:%S")
        last_date: datetime = datetime.strptime(attributes[1], "%Y-%m-%d %H:%M:%S")
        add_transactions: int = int(attributes[2])
        max_churn_index: float = float(attributes[3])
        max_share_transactions: float = float(attributes[4])
        max_margin_call: float = float(attributes[5])

        first_date, last_date = self.get_first_last_date(first_date=first_date, last_date=last_date)
        difference: int = (last_date - first_date).days

        between_date: pyspark.sql.DataFrame = self.repo.create_between_date(first_date=first_date, last_date=last_date,
                                                                            date_dif=difference,
                                                                            add_transactions=add_transactions)
        grouped_data: pyspark.sql.DataFrame = self.repo.get_grouped_data(max_churn_index=max_churn_index,
                                                                         max_share_transactions=max_share_transactions)
        min_discount: pyspark.sql.DataFrame = self.repo.create_min_discount()
        between_date = self.repo.get_final_between_date(between_date=between_date, grouped_data=grouped_data,
                                                        min_discount=min_discount, max_margin_call=max_margin_call)

        between_date.show()

    def get_cross_sell_offer(self, atrs: str):
        attributes: List[str] = atrs.split(",")
        if StaticService.check_atrs_len(atrs=attributes, need_len=5) or StaticService.check_validate_sum_cs(
                atrs=attributes, need_sum=5):
            StaticService.print_error_inc_values()
            return

        group_amount: int = int(attributes[0])
        max_churn_ind: float = float(attributes[1])
        max_stab_ind: float = float(attributes[2])
        max_sku_percent: float = float(attributes[3])
        max_margin_call: float = float(attributes[4])

        client_groups: pyspark.sql.DataFrame = self.repo.create_client_groups(group_amount=group_amount)
        user_good_groups: pyspark.sql.DataFrame = self.repo.create_good_groups(
            max_churn_ind=max_churn_ind, max_stab_ind=max_stab_ind)
        user_groups: pyspark.sql.DataFrame = self.repo.find_user_groups(user_good_groups=user_good_groups,
                                                                        client_groups=client_groups)

        max_diff_val: float = float(self.repo.get_max_sku_diff())
        max_diff_id: str = self.repo.get_max_sku_id(max_diff_val=max_diff_val)
        count_diff: Tuple[str, str] = self.repo.get_sku_id_amount(sku_id=max_diff_id)
        sku_ratio: float = min(round(float(count_diff[1]) / float(count_diff[0]), 2), max_sku_percent / 100)

        selected_prices: pyspark.sql.Row = self.repo.get_sku_prices(sku_id=max_diff_id)
        retail_price: float = float(selected_prices.collect()[0][0])
        purchase_price: float = float(selected_prices.collect()[0][1])
        margin_call_ratio = max_margin_call / 100 * (retail_price - purchase_price) / retail_price

        user_groups = self.repo.find_offer_disc_depth(dataframe=user_groups, margin_call_ratio=margin_call_ratio)
        user_groups.show()

    def get_first_last_date(self, first_date: datetime, last_date: datetime) -> Tuple[datetime, datetime]:
        dates: Tuple[datetime, datetime] = self.repo.get_min_max_dates()
        min_date_dt: datetime = dates[0]
        max_date_dt: datetime = dates[-1]

        if first_date < min_date_dt or first_date > max_date_dt:
            first_date = min_date_dt

        if last_date > max_date_dt or last_date < min_date_dt:
            last_date = max_date_dt

        if first_date > last_date:
            first_date, last_date = last_date, first_date

        return first_date, last_date
