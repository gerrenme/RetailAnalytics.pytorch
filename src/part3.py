import os


def check_user_status() -> bool:
    user_role: str = os.environ.get("USER_ROLE", "READ_ONLY")
    return user_role == "ADMIN"
