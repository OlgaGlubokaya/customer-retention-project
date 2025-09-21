"""
Назва файлу: assessment_of_causal_effect_loss_num.py

Мета:
Оцінка впливу підняття таргетів з виконання показників якості (Learning KPIs і Business KPIs)
на зниження відтоку клієнтів за допомогою логістичної регресії.

Опис:
- Створює бінарну змінну treatment (було підняття таргетів чи ні).
- Визначає outcome: чи знизився відтік (1 – так, 0 – ні) відносно медіани loss_number_normalized.
- Виконує логістичну регресію (Logit-модель) з контролем за коефіцієнтом зацікавленості викладача (mean_chance).
- Обчислює:
    * ATE (логіт)
    * Odds Ratio (OR)
    * Ймовірності p0 та p1 (без і з підняттям таргетів)
    * Абсолютне зменшення відтоку (%)
- Генерує готовий висновок у текстовому форматі.
- Зберігає результати в CSV для подальшої візуалізації або використання у звітах.

Вхідні дані:
- CSV-файл data/results_of_modeling/Bonus_v1_v2.csv з колонками:
  * targets_achieved – факт підняття таргетів (0/1 або кількість)
  * loss_number_normalized – нормалізований показник відтоку
  * mean_chance – середньомісячний коефіцієнт зацікавленості викладача

Вихідні дані:
- CSV-файл data/results_of_modeling/targets_uplift_effect.csv з метриками:
  * ATE (logit)
  * Odds Ratio
  * p0, p1 (ймовірності)
  * Абсолютне зменшення відтоку (%)
  * Автоматично згенерований текст висновку
"""

import pandas as pd
import statsmodels.api as sm
import numpy as np

# Завантажуємо дані
df = pd.read_csv("data/results_of_modeling/Bonus_v1_v2.csv")

# Treatment: підняття таргетів по виконанню показників якості
df["treatment"] = (df["targets_achieved"] > 0).astype(int)

# Outcome: чи знизились відвали (1 - так, 0 - ні)
mean_coef = df["loss_number_normalized"].median()   # приклад: таргет = нижче медіани
df["outcome"] = (df["loss_number_normalized"] < mean_coef).astype(int)

# Контрольна змінна – середнемісячний коєфіціент зацікавленності викладача (відношення
# суми бонуса до суми загального дохода)
controls = ["mean_chance"]

# Формуємо X і y
X = df[["treatment"] + controls]
X = sm.add_constant(X)  # додаємо константу
y = df["outcome"]

# Логістична регресія
model = sm.Logit(y, X)
result = model.fit(disp=0)

print(result.summary())

# Оцінка ефекту treatment
ate = result.params["treatment"]
print(f"Оцінка ефекту підняття таргетів по виконанню показників якості (ATE, логіт): {ate:.3f}, OR = {np.exp(ate):.2f}")

# Вхідні дані з моделі
beta_0 = -0.5341   # константа
beta_1 = 0.905     # ефект treatment (targets_achieved)

# 1. Ймовірність без бонусу
p0 = 1 / (1 + np.exp(-beta_0))

# 2. Ймовірність з бонусом
p1 = 1 / (1 + np.exp(-(beta_0 + beta_1)))

# 3. Абсолютне збільшення у відсотках
abs_increase = (p1 - p0) * 100

# 4. Odds Ratio
odds_ratio = np.exp(beta_1)

# 5. Генерація готової фрази для титульного листа
title_text = (
    f"Запропоновано систему підняття таргетів по виконанню LKPI i BKPI, "
    f"яка знижує відтік клієнтів у {odds_ratio:.2f} рази "
    f"(абсолютне збільшення ≈ {abs_increase:.1f}%)."
)

# 6. Друк результатів
print(f"p0 (без бонусу v1): {p0:.3f} ({p0*100:.1f}%)")
print(f"p1 (з бонусом v1): {p1:.3f} ({p1*100:.1f}%)")
print(f"Odds Ratio (OR): {odds_ratio:.2f}")
print(f"Абсолютне збільшення ймовірності: {abs_increase:.1f}%")
print("\nВисновок:")
print(title_text)

# 7. Збереження результатів
results = {
    "ATE (logit)": ate,
    "Odds Ratio": odds_ratio,
    "p0 (без підняття таргетів)": p0,
    "p1 (з підняттям таргетів)": p1,
    "Absolute Decrease in Loss (%)": abs_increase,
    "Conclusion": title_text
}

# 8. Перетворюємо словник у DataFrame і зберігаємо
results_df = pd.DataFrame(list(results.items()), columns=["Metric", "Value"])
results_df.to_csv("data/results_of_modeling/targets_uplift_effect.csv", index=False, encoding="utf-8-sig")

print("✅ Результати збережено у data/results_of_modeling/targets_uplift_effect.csv")
