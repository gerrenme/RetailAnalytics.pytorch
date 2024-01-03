from terminal_view import Application
import os

os.environ["USER_ROLE"] = "ADMIN"  # установка роли пользователя

if __name__ == "__main__":
    app: Application = Application()
    app.run()
