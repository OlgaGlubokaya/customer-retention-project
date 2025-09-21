"""
Назва файлу: assessment_of_causual_effect_bonusV1.py

Мета:
Оцінка впливу запровадження бонусної системи Bonus_v1 на досягнення викладачами цільових показників
(Learning KPIs та Business KPIs) за допомогою логістичної регресії.

Опис:
- Формуємо бінарну змінну treatment (отримав Bonus_v1 чи ні).
- Формуємо бінарний outcome (досягнув коефіцієнта вище середнього).
- Виконуємо логістичну регресію (Logit-модель) з контролем за Bonus_v2.
- Обчислюємо оцінку ефекту (ATE) та Odds Ratio (OR).
- Розраховуємо ймовірності досягнення KPI з/без Bonus_v1 та абсолютне збільшення ймовірності.
- Генеруємо короткий висновок для звіту.
- Зберігаємо результати у CSV для подальшої візуалізації або інтеграції у звіти.

Вхідні дані:
- CSV-файл "data/results_of_modeling/Bonus_v1_v2.csv" з колонками:
  * Bonus_v1 – розмір бонусу першої версії
  * Bonus_v2 – розмір бонусу другої версії
  * coef_achieved_targets – коефіцієнт досягнення цільових показників

Вихідні дані:
- CSV-файл "data/results_of_modeling/bonus_v1_effect.csv" з підсумковими метриками:
  * ATE (logit)
  * Odds Ratio
  * Ймовірності p0, p1
  * Абсолютне збільшення ймовірності (%)
  * Автоматично згенерований текст висновку
"""

import pandas as pd
import statsmodels.api as sm
import numpy as np

# Завантажуємо дані
df = pd.read_csv("data/results_of_modeling/Bonus_v1_v2.csv")

# Treatment: отримав Bonus_v1 чи ні
df["treatment"] = (df["Bonus_v1"] > 0).astype(int)

# Outcome: target
mean_coef = df["coef_achieved_targets"].mean()
df["outcome"] = (df["coef_achieved_targets"] > mean_coef).astype(int)

# Контрольні змінні
controls = ["Bonus_v2"]

# Формуємо матрицю X (treatment + controls) та вектор y (outcome)
X = df[["treatment"] + controls]
X = sm.add_constant(X)  # додаємо константу для регресії
y = df["outcome"]

# Логістична регресія для бінарного outcome
model = sm.Logit(y, X)
result = model.fit(disp=0)

# Друкуємо результати
print(result.summary())

# Оцінка ефекту treatment
ate = result.params["treatment"]
print(f"Оцінка ефекту Bonus_v1 (ATE, логіт): {ate:.3f}, OR = {np.exp(ate):.2f}")

# Вхідні дані з моделі
beta_0 = -2.2541   # константа
beta_1 = 1.3440     # ефект treatment (Bonus_v1)

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
    f"Запропоновано систему бонусації Bonus_v1, "
    f"яка підвищує ймовірність досягнення викладачами Learning KPIs і Business KPIs більш ніж у {odds_ratio:.2f} рази "
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
    "p0 (без бонусу)": p0,
    "p1 (з бонусом)": p1,
    "Absolute Increase (%)": abs_increase,
    "Conclusion": title_text
}

# 8. Перетворюємо словник у DataFrame і зберігаємо
results_df = pd.DataFrame(list(results.items()), columns=["Metric", "Value"])
results_df.to_csv("data/results_of_modeling/bonus_v1_effect.csv", index=False, encoding="utf-8-sig")

print("✅ Результати збережено у data/results_of_modeling/bonus_v1_effect.csv")