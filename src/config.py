from typing import Dict


class Colors:
    RESET: str = "\033[0m"
    RED: str = "\033[91m"
    GREEN: str = "\033[92m"
    YELLOW: str = "\033[93m"
    BLUE: str = "\033[94m"
    MAGENTA: str = "\033[95m"
    CYAN: str = "\033[96m"
    WHITE: str = "\033[97m"


level_map: Dict[int, str] = {
    0: "Root Menu",
    1: "Read Mode",
    2: "Edit Mode",
    3: "Create Mode",
    4: "Delete Mode",
    5: "Save Mode",
    6: "Personalized Proposal Formation. Average check growth",
    7: "Personalized Proposal Formation. Increase in the frequency of visits",
    8: "Personalized Proposal Formation. Cross-selling"
}

menu_info: Dict[str, str] = {
    "menu_level0": "You are in the root menu. The following options are available to you:"
                   "\n\n1 - Read records"
                   "\n2 - Edit records"
                   "\n3 - Create records"
                   "\n4 - Delete records"
                   "\n5 - Save entities"
                   "\n6 - Personalized Proposal Formation. Average check growth"
                   "\n7 - Personalized Proposal Formation. Increase in the frequency of visits"
                   "\n8 - Personalized Proposal Formation. Cross-selling"
                   "\n\nTo execute a specific command, enter the number corresponding to it",

    "menu_level1": "You are in entity VIEW mode. To view the contents of an entity, enter the following "
                   "parameters:\n\n->\n-> Entity_name\n->\n\n"
                   "The following entities are available for viewing:",

    "chose_read_parameters": "\nEnter the parameters to read the entity "
                             "\n->\n-> Parameter_view(head/tail),number_of_columns_to_view (sep=',')\n->",

    "menu_level2": "You are in entity EDIT mode. To edit the entity record enter the following "
                   "parameters:\n\n->\n-> Entity_name\n->\n\n"
                   "The following entities are available for updating records:",

    "change_row_params": "Enter the new entity record values in the following format: \n->\n-> "
                         "row_id|param1,param2,..,paramN (row_id_sep='|', param_sep=',') \n->",

    "menu_level3": "You are in the mode of ADDING records to entities. To add an entry to an entity, enter the "
                   "parameters:\n\n->\n-> Entity_name\n->\n\n"
                   "The following entities are available for updating records:",

    "add_new_row": "Enter the new entity record values in the following format: \n->\n-> "
                   "row_id,param1,param2,..,paramN (sep=',') \n->",

    "menu_level4": "You are in the mode of DELETING entity records. To delete record in an entity, enter the "
                   "following parameters:\n\n->\n-> Entity_name \n->\n\n"
                   "The following entities are available for deleting records:",

    "delete_row": "Enter the id of the record to delete it",

    "menu_level5": "Enter the name of the entity you want to save. The program will automatically specify the path "
                   "where the data will be overwritten. \n Select and enter the name of the entity you want to save:",

    "menu_level6": "You are in the mode of Forming personalized offers focused on growing your average check. To "
                   "generate personalized offers, enter the following parameters separated by commas: \n->\n-> "
                   "calculation_method_median_check(1 - for period, 2 - last transactions),first_date(1st method),"
                   "second_date(1st method),amount_of_transactions(2nd method),\n\taverage_check_inc_rate,"
                   "max_churn_ind,max_share_disc_transactions(in percent),allowable_share_margin(in percent)\n->\n",

    "menu_level7": "You are in the mode of Generating personalized offers focused on increasing visit frequency. To "
                   "create personalized offers, enter the following parameters:\n->\n-> first_date,last_date,"
                   "add_transactions,max_churn_index(int percent),max_share_discount(in percent),"
                   "max_margin_call(in percent)\n->\n",

    "menu_level8": "You are in the Cross-Sell Personalized Offer Formation mode. To generate personalized offers, "
                   "enter the following parameters:\n->\n-> groups_amount,max_outflow_index,"
                   "max_consumption_stability_index,max_sku_percent,max_margin_call(sep=',')\n->\n",
}

system_info: Dict[str, str] = {
    "enter_incorrect_values": "You have entered incorrect values. Repeat the entry",

    "not_amount_attrs": "The number of attributes received does not match the number of columns in the entity. "
                        "The record modification operation is canceled",

    "no_such_command": "No similar command exists. Re-enter",

    "not_enough_roots": "You do not have sufficient permissions to perform such an operation",

    "success": "Operation successful",

    "give_entity_name": "Specify the name of the entity you want to perform the operation on",
}
