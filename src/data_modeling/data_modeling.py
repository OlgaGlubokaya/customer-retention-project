'''
Назва файлу: data_modeling.py

Мета:
-----
Цей скрипт виконує повний цикл аналізу даних щодо викладачів, розраховує коефіцієнти мотивації,
моделює бонуси за гіпотезами, а також будує прогноз досягнення таргетів за допомогою моделі
RandomForestClassifier. 

Функціонал:
-----------
1. Завантаження даних:
   - Імпорт даних з `data/Teacher_indicators.csv`.
   - Розрахунок коефіцієнта мотивації викладача `chance` як частки бонусу в загальному доході.

2. Попередня обробка: 
   - Розрахунок помісячного середнього значення мотивації (`mean_chance`).
   - Розрахунок частки досягнутих таргетів на місяць (`coef_achieved_targets`).
   - Збереження обробленого DataFrame у CSV.

3. Формування гіпотез:  
   - Гіпотеза 1: нарахування бонусу пропорційно до виконання ключових показників KPI
     (фідбеки, успішність, контроль домашки, втрати).  
   - Гіпотеза 2: нарахування бонусу лише якщо виконані всі умови; фіксований мінімум бонусу.

4. Описова статистика: 
   - Розрахунок медіани, середнього та максимуму коефіцієнта досягнення таргетів.  
   - Збереження результатів у `data/results_of_modeling/Coef_achieved_targets_stat.csv`.

5. Підготовка фіч для моделі:  
   - Використані фічі: `Bonus`, `Bonus_v1`, `Bonus_v2`.
   - Формування цільової змінної `target` (1 — викладач вище середнього за досягненням таргетів, 0 — нижче).

6. Моделювання:  
   - Навчання моделі `RandomForestClassifier`.
   - Оцінка точності (`accuracy`) та площі під ROC-кривою (`roc_auc`).
   - Побудова звіту за precision, recall, f1-score.
   - Збереження результатів у `data/results_of_modeling/Classification_results.csv`.

7. Аналіз важливості фіч:
   - Розрахунок ваг ознак за допомогою `feature_importances_`.
   - Збереження у `data/results_of_modeling/Classification_importances_results.csv`.


Вхідні дані:
---------------------
- "data/Teacher_indicators.csv" - датасет з сайту аналітики компанії, який містить часові ряди з щомісячними
показниками виконання викладачами KPI у відсотковому відношенні, інформацію стосовно відвалі викладачів,
фінансові і кількісні показники для розрахунку загального доходу викладачів, що включає базовий дохід і бонус
за умов, прийнятих і існуючих в компанії.
- "docs/Teacher_indicators_dataset.md" - опис показників датасету Teacher_indicators.csv.
- "docs/Teacher_teachers_compensation_system_hypotheses.md" - опис існуючої системи оплати праці і бонусації,
яка прийнята в компанії і має назву в цьому проєкті "Bonus"

Вихідні дані:
---------------------

- "data/results_of_modeling/Teacher_indicators_chance.csv" - датасет, який додатково містить коєфіцієнти 
зацікавленості викладачів у досягненні KPI;
- "data/results_of_modeling/Bonus_v1_v2.csv" - датасет, в який додано отримані бонуси за двома запропонованими 
системами бонусації "Bonus_1" i "Bonus_2";
- "data/results_of_modeling/Coef_achieved_targets_stat.csv" -  датасет містить коєфіцієнти зацікавленості 
викладачів в досягненні таргетів (KPI);
- "data/results_of_modeling/Classification_results.csv" - результати застосованого алгоритму RandomForestClassifier;
- "data/results_of_modeling/Classification_importances_results.csv" - оцінка важливості запропонованих систем
бонусації на підвищення мотивації викладачів. 

'''

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, roc_auc_score, classification_report

# 1. Завантажуємо дані
df = pd.read_csv("data/Teacher_indicators.csv")
# Оцінимо мотивацію викладача виконувати показники якості, які є додатковими до їх основної зар. платні
# через такий параметр:
# 0 бонус = 0 мотивації
# х бонус = х мотивація
# Параметр мотивації грунтується на ствердженні матеріальної зацікавленності викладача досягати показникі якості. 
# Порахуємо коефіціент зацікавленості викладача в отриманні бонусу, по формулі:
# h = Bonus /  Total_compensation,
# де h - коефіціент зацікавленості викладача, тобто частка бонусу в загальному доході викладача
# h = df["chance"]

df["chance"] = round (df["Bonus"] / df["Total_compensation"], 2)

# Порахуємо щомісячне середнє значення мотивації викладачів (не враховуючи нульові значення)
mean_chance = (
    df.loc[df["chance"] > 0]
    .groupby("Month")["chance"]
    .mean()
    .round(2)
)

# Додаємо колонку "mean_chance" в основний df
df["mean_chance"] = df["Month"].map(mean_chance)

# Якщо залишились NaN після map — замінимо їх на 0 (або можна середнім значенням)
df["mean_chance"] = df["mean_chance"].fillna(0)

# Порахуємо помісячно удельний вес досягнутих таргетів в загальної кількості 
# помісячних записів (працюючих викладачів).
target_weights = (
    df.groupby('Month')['targets_achieved']
      .mean()  # середнє по групі = частка 1 (таргет досягнуто) у загальній кількості записів щомісячно
      .reset_index()
      .rename(columns={'targets_achieved': 'coef_achieved_targets'})
      .round(2)
)

# Додаємо цей стовпець до основного df
df = df.merge(target_weights, on='Month', how='left')

# Якщо залишились NaN після map — замінимо їх на 0 (або можна середнім значенням)
df["coef_achieved_targets"] = df["coef_achieved_targets"].fillna(0)
df.to_csv("data/results_of_modeling/Teacher_indicators_chance.csv", index=False)


# Будуємо модель RandomForestClassifier. Для цього треба розрахувати значення параметрів - фіч,
# які будуть впливати на таргет (підвищення мотивації викладачів). Для цього розрахуємо бонуси по 
# запропонованим гіпотезам Bonus_1 i Bonus_2.

# --- Гіпотеза 1 "Bonus_v1" ---
df["Bonus_v1"] = 0.0
df.loc[df["Feedback_for_parents"] >= 90, "Bonus_v1"] += df["BD"] * 0.111
df.loc[df["Feedback_to_students"] >= 90, "Bonus_v1"] += df["BD"] * 0.126
df.loc[df["Control_of_homework"] >= 90, "Bonus_v1"] += df["BD"] * 0.132
df.loc[df["Control_of_potential_loss"] >= 85, "Bonus_v1"] += df["BD"] * 0.130
df.loc[df["Average_success"] >= 75, "Bonus_v1"] += df["BD"] * 0.132

df["Bonus_v1"] = df["Bonus_v1"].round(2)

df["Total_income_v1"] = round(df["BD"] + df["Bonus_v1"], 2)

# --- Гіпотеза 2 "Bonus_v2"---
def bonus_v2(row):
    conds = [
        row["Feedback_for_parents"] >= 90,
        row["Feedback_to_students"] >= 90,
        row["Control_of_homework"] >= 90,
        row["Control_of_potential_loss"] >= 85,
        row["Average_success"] >= 75
    ]
    if all(conds):
        return max(row["rate"] * row["lesson_count"], 1000)
    else:
        return 0.0

df["Bonus_v2"] = df.apply(bonus_v2, axis=1)
df["Total_income_v2"] = round(df["BD"] + df["Bonus_v2"], 2)
df.to_csv("data/results_of_modeling/Bonus_v1_v2.csv", index=False)

# Порахуємо медіану, середню і максимум по стовпцю "coef_achieved_targets"
median = df["coef_achieved_targets"].median()
max = df["coef_achieved_targets"].max()
mean = df["coef_achieved_targets"].mean()

# Зберігаємо ці статистичні дані в CSV
# Створюємо DataFrame з результатами
stats_df = pd.DataFrame({
    "Metric": ["Median", "Max", "Mean"],
    "Value": [median, max, mean]
})

# Зберігаємо у CSV
stats_df.to_csv("data/results_of_modeling/Coef_achieved_targets_stat.csv", index=False, encoding="utf-8-sig") 

# === Формування фіч для моделі ===
features = [
    "Bonus",   
    "Bonus_v1",   
    "Bonus_v2",    
]
df["target"] = (df["coef_achieved_targets"] > mean).astype(int)

X = df[features]
y = df["target"]


# ===  Тренування моделі ===
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

model = RandomForestClassifier(random_state=42)
model.fit(X_train, y_train)

# ===  Оцінка ===
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

# ===  Формуємо результати   ===
accuracy = accuracy_score(y_test, y_pred)
roc_auc = roc_auc_score(y_test, y_proba)
report = classification_report(y_test, y_pred, output_dict=True)

# Перетворюємо classification_report у DataFrame
report_df = pd.DataFrame(report).T.reset_index()
report_df.rename(columns={"index": "Metric"}, inplace=True)

# Створюємо окремий DataFrame для roc_auc
extra_metrics_df = pd.DataFrame({
    "Metric": ["roc_auc"],
    "value": [roc_auc]
})

# Об’єднуємо все в один фінальний DataFrame
final_results = pd.concat([report_df, extra_metrics_df], ignore_index=True)

# Збереження у CSV 
final_results.to_csv("data/results_of_modeling/Classification_results.csv", index=False)

# === Отримуємо важливості фіч ===
importances = model.feature_importances_
feature_names = X.columns

# Створюємо датафрейм для зручності
feat_importances = pd.DataFrame({
    "Feature": feature_names,
    "Importance": importances
}).sort_values(by="Importance", ascending=False)

# Зберігаємо в .csv
feat_importances.to_csv("data/results_of_modeling/Classification_importances_results.csv", index=False, float_format="%.6f")
