import re


class TypesValidate:

    @staticmethod
    def validate_int(num: str) -> bool:
        pattern: re.Pattern = re.compile(r'^[+-]?\d+$')
        return bool(pattern.match(num))

    @staticmethod
    def validate_float(num: str) -> bool:
        pattern: re.Pattern = re.compile(r'^[+-]?\d+(\.\d+)?$')
        return bool(pattern.match(num))

    @staticmethod
    def validate_name(name: str) -> bool:
        pattern: re.Pattern = re.compile(r'^[A-Z][a-z]+\s[A-Z][a-z]+$')
        return bool(pattern.match(name))

    @staticmethod
    def validate_date(date: str) -> bool:
        pattern: re.Pattern = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
        return bool(pattern.match(date))

    @staticmethod
    def validate_mail(mail: str) -> bool:
        pattern: re.Pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@email\.com$')
        return bool(pattern.match(mail))

    @staticmethod
    def validate_phone(phone_number: str) -> bool:
        pattern: re.Pattern = re.compile(r'^\d{3}-\d{3}-\d{4}$')
        return bool(pattern.match(phone_number))
