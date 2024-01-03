from dataframes import DataClass
from typing import List


class Service:
    def __init__(self) -> None:
        self.dataframes: DataClass = DataClass()

    # Для всех методов ниже - True == ошибка, False == все гуд

    def check_entity_in_map(self, ent_name: str) -> bool:
        if ent_name not in self.dataframes.entity_map:
            return True

        return False

    def check_se_operations(self, rows: int, ent_name: str, read_type: str) -> bool:
        if (rows < 0 or ent_name not in self.dataframes.entity_map or
                self.dataframes.entity_map[ent_name].count() < rows or read_type not in ["head", "tail"]):
            return True

        return False

    def check_ce_atrs(self, atrs: List[str], ent_name: str, row_id: str) -> bool:  # Change Entity
        if (len(atrs) != len(self.dataframes.entity_map[ent_name].columns) - 1 or
                ent_name not in self.dataframes.entity_map or
                self.dataframes.entity_map[ent_name].filter(
                    row_id == self.dataframes.entity_map[ent_name]
                    [self.dataframes.entity_map[ent_name].columns[0]]).count() == 0):

            print(len(atrs) != len(self.dataframes.entity_map[ent_name].columns))
            print(ent_name not in self.dataframes.entity_map)
            print(self.dataframes.entity_map[ent_name].filter(
                    row_id == self.dataframes.entity_map[ent_name]
                    [self.dataframes.entity_map[ent_name].columns[0]]).count() == 0)

            return True

        return False

    def check_ae_atrs(self, atrs: List[str], ent_name: str) -> bool:
        total_columns: int = len(self.dataframes.entity_map[ent_name].columns)
        new_row_id: str = atrs[0]

        if (self.dataframes.entity_map[ent_name].filter(new_row_id == self.dataframes.entity_map[ent_name][
            self.dataframes.entity_map[ent_name].columns[0]]).count() != 0 or
                len(atrs) != total_columns):
            return True

        return False

    def check_de_atrs(self, row_id: str, ent_name: str) -> bool:
        if (ent_name not in self.dataframes.entity_map or
                self.dataframes.entity_map[ent_name].filter(row_id == self.dataframes.entity_map[ent_name][
                    self.dataframes.entity_map[ent_name].columns[0]]).count() == 0):
            return True

        return False
