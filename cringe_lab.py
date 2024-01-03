import math


# Task1

a_value: float = 1378.13
a_error: float = 0.01
b_value: float = 68.00
b_error: float = 0.005

# расчет суммы, разности, произведения и частного
sum_result: float = a_value + b_value
diff_result: float = a_value - b_value
prod_result: float = a_value * b_value
quot_result: float = a_value / b_value

# абсолютные погрешности для каждого результата
sum_error: float = a_error + b_error
diff_error: float = a_error + b_error
prod_error: float = abs(prod_result) * (a_error/a_value + b_error/b_value)
quot_error: float = abs(quot_result) * (a_error/a_value + b_error/b_value)

# возведение в степень и извлечение корня
a_cubed: float = a_value**3
b_cubed_root: float = b_value**(1/3)

# абсолютные и относительные погрешности для результатов степени и корня
a_cubed_error: float = 3 * a_error * a_value**2
b_cubed_root_error: float = (1/3) * b_error * b_value**((1/3)-1)

print(f"сумма: {sum_result}, абсолютная погрешность: {sum_error}, относительная погрешность: "
      f"{sum_error/sum_result * 100}%")

print(f"разность: {diff_result}, абсолютная погрешность: {diff_error}, относительная погрешность: "
      f"{diff_error/diff_result * 100}%")

print(f"произведение: {prod_result}, абсолютная погрешность: {prod_error}, относительная погрешность: "
      f"{prod_error/prod_result * 100}%")

print(f"частное: {quot_result}, абсолютная погрешность: {quot_error}, относительная погрешность: "
      f"{quot_error/quot_result * 100}%")

print(f"возведение a в степень 3: {a_cubed}, абсолютная погрешность: {a_cubed_error}, относительная погрешность: "
      f"{a_cubed_error/a_cubed * 100}%")

print(f"извлечение корня 3 степени из b: {b_cubed_root}, абсолютная погрешность: {b_cubed_root_error}, "
      f"относительная погрешность: {b_cubed_root_error/b_cubed_root * 100}%")


# Task2
# def f(x: float) -> float:
#     return math.tan(x) + x ** 3 - 6 * x ** 2 - 7 * x + 5
#
#
# def bisection_method(a: float, b: float, tol: float, max_iter: int) -> None | float:
#     if f(a) * f(b) > 0:
#         print("не удовлетворяет условиям метода половинного деления")
#         return None
#
#     iter_count = 0
#     while (b - a) / 2 > tol and iter_count < max_iter:
#         c = (a + b) / 2
#         if f(c) == 0:
#             return c
#         elif f(c) * f(a) < 0:
#             b = c
#         else:
#             a = c
#         iter_count += 1
#
#     return (a + b) / 2
#
#
# eps: float = 1e-6
# root1 = bisection_method(a=-2, b=-1, tol=eps, max_iter=1000)
# print("Корень 1:", root1)
# root2 = bisection_method(a=0, b=1, tol=eps, max_iter=1000)
# print("Корень 2:", root2)
# root3 = bisection_method(a=1, b=2, tol=eps, max_iter=1000)
# print("Корень 3:", root3)
