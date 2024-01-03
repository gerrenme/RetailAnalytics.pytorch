from types_validate import TypesValidate
from typing import List
from dataframes import DataClass
from config import Colors, system_info


class StaticService:
    def __init__(self) -> None:
        self.dataframes: DataClass = DataClass()

    # Для всех методов ниже - True == ошибка, False == все гуд
    @staticmethod
    def check_atrs_len(atrs: List[str], need_len: int) -> bool:
        if len(atrs) != need_len:
            return True

        return False

    @staticmethod
    def check_se_atrs(atrs: List[str]) -> bool:
        if StaticService.check_atrs_len(atrs=atrs, need_len=2) or not TypesValidate.validate_int(atrs[-1]):
            return True

        return False

    @staticmethod
    def check_oi_atrs(atrs: List[str]) -> bool:
        calculation_method: str = atrs[0]
        if (calculation_method == "1" and len(atrs) != 7) or (calculation_method == "2" and len(atrs) != 6):
            return True

        return False

    @staticmethod
    def check_validate_sum_oi(atrs: List[str], need_sum: int, balancer: int = 0) -> bool:
        if sum([TypesValidate.validate_float(atrs[3 - balancer]),
                TypesValidate.validate_float(atrs[4 - balancer]),
                TypesValidate.validate_float(atrs[5 - balancer]),
                TypesValidate.validate_float(atrs[6 - balancer])]) != need_sum:
            return True

        return False

    @staticmethod
    def check_validate_sum_oi_date(atrs: List[str], need_sum: int):
        if (sum([TypesValidate.validate_date(atrs[1]), TypesValidate.validate_date(atrs[2])])) != need_sum:
            return True

        return False

    @staticmethod
    def check_validate_sum_if(atrs: List[str], need_sum: int) -> bool:
        if sum([TypesValidate.validate_date(atrs[0]), TypesValidate.validate_date(atrs[1]),
                TypesValidate.validate_int(atrs[2]), TypesValidate.validate_float(atrs[3]),
                TypesValidate.validate_float(atrs[4]), TypesValidate.validate_float(atrs[5])]) != need_sum:
            return True

        return False

    @staticmethod
    def check_validate_sum_cs(atrs: List[str], need_sum: int) -> bool:
        if sum([TypesValidate.validate_int(atrs[0]), TypesValidate.validate_float(atrs[1]),
                TypesValidate.validate_float(atrs[2]), TypesValidate.validate_float(atrs[3]),
                TypesValidate.validate_float(atrs[4])]) != need_sum:
            return True

        return False

    @staticmethod
    def print_error_inc_values():
        print(Colors.RED + system_info["enter_incorrect_values"] + Colors.RESET)

    @staticmethod
    def print_error_not_amount_attrs():
        print(Colors.RED + system_info["not_amount_attrs"] + Colors.RESET)

    @staticmethod
    def print_error_no_command():
        print(Colors.RED + system_info["no_such_command"] + Colors.RESET)

    @staticmethod
    def print_error_no_roots():
        print(Colors.RED + system_info["not_enough_roots"] + Colors.RESET)

    @staticmethod
    def print_give_entity():
        print(Colors.RED + system_info["give_entity_name"] + Colors.RESET)

    @staticmethod
    def print_success():
        print(Colors.CYAN + system_info["success"] + Colors.RESET)
