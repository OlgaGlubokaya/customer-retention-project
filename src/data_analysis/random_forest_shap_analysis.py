"""
Назва файлу: random_forest_shap_analysis.py

Мета:
-----
Аналіз впливу відсотку виконання викладачами Business KPI i Learning KPI на річний відсоток відвалів
кожного викладача за допомогою моделі RandomForestRegressor та інтерпретація результатів
через SHAP-значення (Shapley Additive Explanations). 

Business KPI:
    - "Feedback_for_parents" - щотижневий звіт в батьківський чат групи, оцінюється щомісячно у відсотках,
    - "Feedback_to_students"- щотижневий звіт в учнівський чат групи, оцінюється щомісячно у відсотках,
    - "Control_of_potential_loss" - - щотижневий звіт тьютору з описом дітей, які показують незацікавленість
    у подальшому навчанні, оцінюється щомісячно у відсотках.

Learning KPI:
    - "Control_of_homework" - щотижнева перевірка Домашньої практики і обговорення зі судентами в
    учнівському чаті, оцінюється щомісячно у відсотках,
    -  "Average_success" - щотижнева перевірка успішності дітей на платформі Learning Management System (lms),
    оцінюється щомісячно у відсотках. 

В моделі використовуються фактори (фічі) важливості виконання кожного KPI в кількості відвалів по кожному
викладачу. Дані важливості розраховані в модулі "Data_Analysis/feature_engineering.py", функцією
"add_quality_importance(df)", де Importance - це частка кожного KPI у відвалах, розрахованих помісячно.

Файл призначений для:
1. Побудови моделі прогнозування відвалів (loss_number_normalized) на основі відсотка виконання викладачами 
ключових показників ефективності (освітні і бізнес метрики).
2. Оцінки якості моделі за допомогою метрик R², MAE та RMSE.
3. Інтерпретації впливу ознак на результат через SHAP.
4. Збереження метрик, таблиць важливості та графічної візуалізації.

Основні кроки:
--------------
1. Завантаження даних з файлу `Analysis_teacher_with_importance.csv`.
2. Вибір ознак (features) та цільових змінних (targets).
3. Розбиття даних на тренувальну та тестову вибірки.
4. Навчання RandomForestRegressor.
5. Обчислення метрик моделі:
   - R² (коефіцієнт детермінації)
   - MAE (середня абсолютна похибка)
   - RMSE (корінь середньої квадратичної похибки)
6. Використання SHAP для інтерпретації моделі:
   - Summary Plot (розсіювання за впливом ознак)
   - Bar Plot (середній внесок ознак, нормалізований до шкали 0–1)
7. Збереження результатів:
   - Таблиці метрик: `metrics_<target>.csv`
   - Таблиці важливості ознак: `importance_<target>.csv`
   - Графіки SHAP (summary та bar): `images/plots_RandomForest/`

Вхідні дані:
---------
    - "data/Analysis_teacher_with_importance.csv" - датасет, що містить часові ряди викладачів по
    показникам якості і частки важливості кожного KPI в ставшихся відвалах викладачів.

Вихідні дані:
---------
     - "data/results_RandomForest/metrics_loss_number_normalized.csv" - таблиця метрик RandomForestRegressor:
            R2 - коефіцієнт детермінації, показує, наскільки добре модель пояснює варіацію таргета,
            MAE - Mean Absolute Error, середня абсолютна помилка, показує відхилення прогнозу від реального значення,
            RMSE - Root Mean Squared Error, корінь середньоквадратичної помилки з акцентом на великі відхилення.
     - "data/results_RandomForest/importance_loss_number_normalized.csv" - таблиця важливості
        ознак  Feature, SHAP value нормалізовані,
     - "images/plots_RandomForest/shap_summary_loss_number_normalized.png" - графік, який показує розподіл
        значень важливості кожної ознаки відносно впливу на кількість відвалів,
     - "images/plots_RandomForest/shap_bar_loss_number_normalized.png" - графік, показує нормалізовані значення
       SHAP values по кожної фічі впливу.
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import shap
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split

# =====================
# Параметри
# =====================
features = [
    "importance_of_parents",
    "importance_of_students",
    "importance_of_homework",
    "importance_of_loss",
    "importance_of_success"
   ]

targets = ["loss_number_normalized"]

# Завантаження даних
df = pd.read_csv("data/Analysis_teacher_with_importance.csv")  

# =====================
# Аналіз для кожного таргета
# =====================
for target in targets:
    X = df[features]
    y = df[target]

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Модель
    model = RandomForestRegressor(
        n_estimators=50,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Прогноз на тестових даних
    y_pred = model.predict(X_test)

    # Обчислення метрик
    r2 = r2_score(y_test, y_pred)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))

    # SHAP
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)

    # Таблиця важливостей
    importance_df = pd.DataFrame({
        "Feature": features,
        "Mean_SHAP_Abs": np.abs(shap_values).mean(axis=0).round(6)
    }).sort_values(by="Mean_SHAP_Abs", ascending=False)

    # ====== Збереження результатів ======
    metrics_df = pd.DataFrame({
        "Target": [target],
        "R2": [round(r2, 3)],
        "MAE": [round(mae, 3)],
        "RMSE": [round(rmse, 3)]
    })

    # Зберігаємо метрики
    metrics_df.to_csv(f"data/results_RandomForest/metrics_{target}.csv", index=False, encoding="utf-8-sig")

    # Зберігаємо важливості
    importance_df.to_csv(f"data/results_RandomForest/importance_{target}.csv", index=False, encoding="utf-8-sig")

    # ====== Графіки ======
    # Summary plot
    plt.figure()
    shap.summary_plot(shap_values, X, show=False)
    plt.title(f"SHAP Summary Plot: {target}")
    plt.savefig(f"shap_summary_{target}.png", bbox_inches="tight", dpi=300)
    plt.close()

    # Bar plot
    plt.figure()
    shap.summary_plot(shap_values, X, plot_type="bar", show=False)
    plt.title(f"SHAP Bar Plot: {target}")
    plt.savefig(f"shap_bar_{target}.png", bbox_inches="tight", dpi=300)
    plt.close()

print("\n✅ Аналіз завершено. Графіки та таблиці збережено.")
