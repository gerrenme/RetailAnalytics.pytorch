from part3 import check_user_status
from config import menu_info, Colors, level_map
from controller import Controller
from static_service import StaticService


class Application:
    def __init__(self):

        self.controller: Controller = Controller()
        self.current_terminal_level: int = 0
        self.current_command: str = ""

    def withdraw_card(self) -> None:
        print("\n")
        print(menu_info[f"menu_level{self.current_terminal_level}"])
        self.print_entities()

        print(Colors.MAGENTA + f"\nYou're status: {'Admin' if check_user_status() else 'User'}. Current level is "
                               f"{level_map[self.current_terminal_level]}. To exit to"
                               f"the main menu, type 'b'. to quite program, type 'q'" + Colors.RESET)

    def print_entities(self) -> None:
        self.controller.print_entities(terminal_level=self.current_terminal_level)

    def show_entity(self, entity_name: str) -> None:
        self.controller.show_entity(entity_name=entity_name)

    def change_row(self, entity_name: str) -> None:
        self.controller.change_entity_row(entity_name=entity_name)

    def add_row(self, entity_name: str) -> None:
        self.controller.add_entity_row(entity_name=entity_name)

    def delete_row(self, entity_name: str) -> None:
        self.controller.delete_entity_row(entity_name=entity_name)

    def save_entity(self, entity_name: str) -> None:
        self.controller.save_entity(entity_name=entity_name)

    def get_offer_check(self, attributes: str):
        self.controller.get_offer_incr_check(atrs=attributes)

    def get_offer_freq(self, attributes: str):
        self.controller.get_offer_incr_freq(atrs=attributes)

    def get_cs_offer(self, attributes: str):
        self.controller.get_cross_sell_offer(atrs=attributes)

    def get_command(self) -> None:
        self.current_command = input()

        if self.current_command == "b":
            self.current_terminal_level = 0
            return

        if self.current_command == "q":
            print(Colors.RED + "You have terminated the program" + Colors.RESET)
            return

        if self.current_terminal_level == 0:
            if self.current_command not in ["1", "2", "3", "4", "5", "6", "7", "8", ]:
                StaticService.print_error_no_command()

            else:
                self.current_terminal_level = int(self.current_command)

            return

        elif self.current_terminal_level == 1:
            entity_name: str = self.current_command
            self.show_entity(entity_name=entity_name)

        elif self.current_terminal_level == 6:
            self.get_offer_check(attributes=self.current_command)

        elif self.current_terminal_level == 7:
            self.get_offer_freq(attributes=self.current_command)

        elif self.current_terminal_level == 8:
            self.get_cs_offer(attributes=self.current_command)

        else:
            if not check_user_status():
                StaticService.print_error_no_roots()
                return

        if self.current_terminal_level == 2:
            self.change_row(entity_name=self.current_command)

        elif self.current_terminal_level == 3:
            entity_name: str = self.current_command
            self.add_row(entity_name=entity_name)

        elif self.current_terminal_level == 4:
            entity_name: str = self.current_command
            self.delete_row(entity_name=entity_name)

        elif self.current_terminal_level == 5:
            self.save_entity(entity_name=self.current_command)

    def run(self):
        while self.current_command.lower() != "q":
            self.withdraw_card()
            self.get_command()
