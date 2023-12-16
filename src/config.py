from typing import List, Dict


class Colors:
    RESET: str = '\033[0m'
    RED: str = '\033[91m'
    GREEN: str = '\033[92m'
    YELLOW: str = '\033[93m'
    BLUE: str = '\033[94m'
    MAGENTA: str = '\033[95m'
    CYAN: str = '\033[96m'
    WHITE: str = '\033[97m'


menu_info: Dict[str, str] = {
    "root_menu": "You are in the root menu. The following options are available to you:"
                 "\n\n1 - Read records"
                 "\n2 - Edit records"
                 "\n3 - Create records"
                 "\n4 - Delete records"
                 "\n\nTo execute a specific command, enter the number corresponding to it",

    "first_level": "You are in entity VIEW mode. To view the contents of an entity, enter the following parameters:\n\n"
                   "------------------------/// Entity_name-parameter_view(head/tail)-number_of_columns_to_view "
                   "///------------------------\n\n"
                   "The following entities are available for viewing:",

    "second_level": "You are in the entity record UPDATE mode. To update an entity record, enter the following "
                    "parameters:\n\n"
                    "------------------------/// Entity_name-record_id-new_attributes_values(sep=',')"
                    "///------------------------\n\n"
                    "The following entities are available for updating records:",

    "third_level": "You are in the mode of ADDING records to entities. To add an entry to an entity, enter the "
                   "following parameters:\n\n"
                   "------------------------/// Entity_name-record_id-attributes_for_addition(sep=',')"
                   "///------------------------\n\n"
                   "The following entities are available for adding records:",

    "forth_level": "You are in the mode of DELETING entity records. To delete record in an entity, enter the "
                   "following parameters:\n\n"
                   "------------------------/// Entity_name-record_id"
                   "///------------------------\n\n"
                   "The following entities are available for deleting records:",

    "enter_incorrect_values": "You have entered incorrect values. Repeat the entry",

    "not_amount_attrs": "The number of attributes received does not match the number of columns in the entity. "
                        "The record modification operation is canceled",

    "no_such_command": "No similar command exists. Re-enter",

    "not_enough_roots": "You do not have sufficient permissions to perform such an operation",

    "success": "Operation successful"
}
