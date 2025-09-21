"""
Назва файлу: feature_engineering.py

Мета:
------
Автоматизація аналізу діяльності викладачів на основі даних з CSV та SQLite бази.
Файл формує і поєднує дві категорії викладачів: категорію стабільності і категорію виконання KPI.
Також обчислює наступні аналітичні показники:
    - кількість місяців роботи викладача,
    - витяг даних з аналітичної бази даних компанії, де зберігаються поточні дані про стан виконання
    KPI викладачами,
    - будування часових рядів по викладачам і їх загальним показникам роботи, 
    - нормалізація відвалів по кожному викладачу (помісячний розподіл),
    - питому вагу кожного показника якості викладача в їх загальної якості,
    - коефіцієнт щомісячної важливості кожного KPI в щомісячної кількості відвалів викладача 
    
Призначення:
------------
1. Витяг даних про викладачів із CSV та SQLite.
2. Формування категорій викладачів:
   - teachers_A — працювали два роки в одній категорії (best, interquart, bad, worst).
   - teachers_B — потрапляли у різні категорії за різні роки.
   - teachers_C — хоча б один рік працювали в категорії "best".
   - teachers_D — хоча б один рік працювали в категорії "bad".
3. Об’єднання аналітичних показників із даними про відвали учнів.
4. Обчислення:
   - кількості місяців роботи викладача;
   - сумарної кількості відвалів;
   - нормалізації кількості відвалів на місяць роботи;
   - питомої ваги показників якості (feedback, контроль, успішність) з урахуванням відвалів;
   - коефіцієнт важливості показника якості в щомісячної кількості відвалів по кожному викладачу.
5. Збереження результатів у проміжні CSV-файли для подальшої аналітики.

Кінцевий результат:
-------------------
CSV-файл `Analysis_teacher_with_importance.csv` із наступною інформацією:
- аналітичні показники викладачів;
- кількість відвалів (загальна та нормалізована);
- коефіцієнти якості;
- питомі ваги показників (importance_of_pr, importance_of_st, importance_of_hm, importance_of_pl, importance_of_as).

Структура:
----------
- Константи з ключовими колонками та мапами скорочень.
- Хелпер-функції для збереження груп, роботи з SQL, об’єднання даних, розрахунку показників.
- Основна функція `main()`, що виконує весь пайплайн.

Вхідні дані:
---------
    - "data/teachers_analysis.csv" - дані, отримані з попереднього модуля підготовки даних;
    - "data/teacher_analysis.sqlite" - база даних школи з поточною інформацією про щомісячне
       виконання викладачами Business KPI i Learning KPI;
    - "data/General_Gone_Clients_Report.csv" - датасет з інформацією по відвалам клієнтів, отриманий 
       з попереднього модуля підготовки даних;

Вихідні дані:
---------
     - "data/Analysis_teachers_rate.csv" - датасет для вивантаження даних з бази даних "teacher_analysis.sqlite"
     - "data/teachers_A.csv" - викладачі категорії А - стабільні - працювали підряд два роки в однієї категорії
     - "data/teachers_B.csv" - викладачі категорії B - не стабільні - працювали підряд два роки в різних категоріях
     - "data/teachers_C.csv" - викладачі категорії C - викладачі працювали тільки рік в категорії "best"
     - "data/teachers_D.csv" - викладачі категорії D - викладачі працювали тільки рік в категорії "bad"
     - "data/Analysis_teacher_with_importance.csv" - датасет, що містить часові ряди викладачів по
     показникам якості і частки важливості кожного KPI в ставшихся відвалах викладачів.

"""

import pandas as pd
import sqlite3
from itertools import combinations


# ======================
# Константи
# ======================

# Константи для визначення 4 категорій стабільності викладачів: 
# A - стабільні (два роки в однієї категорії),
# B - нестабільні (два роки в різних категоріях),
# С - один рік в категорії "best",
# D - один рік в категорії "bad".
TEACHER_GROUPS = {
    "A": (lambda df: (df["best"] == 2) | (df["interquart"] == 2) | (df["bad"] == 2) | (df["worst"] == 2)),
    "B": None,  # обробляється окремо
    "C": (lambda df: df["best"] == 1),
    "D": (lambda df: df["bad"] == 1),
}

# Константи для визначення категорій якості викладачів по кількості річних відвалів на кожного по однієї
# групі
CATEGORY_COLS = ["best", "interquart", "bad", "worst"]

# Константи з показниками якості викладачів, які прийнято в компанії:
# Business KPI:
#   - "Feedback_for_parents",
#   - "Feedback_to_students",
#   - "Control_of_potential_loss"
# Learning KPI:
#   - "Control_of_homework",
#   -  "Average_success"
QUALITY_COLS = [
    "Feedback_for_parents",
    "Feedback_to_students",
    "Control_of_homework",
    "Control_of_potential_loss",
    "Average_success"
]

# Скорочені назви показників якості викладачів
QUALITY_SHORT = {
    "Feedback_for_parents": "parents",
    "Feedback_to_students": "students",
    "Control_of_homework": "homework",
    "Control_of_potential_loss": "loss",
    "Average_success": "success"
}

# ======================
# Хелпер-функції
# ======================
def save_teacher_group(df, condition, filename):
    """
    Функція для сегментації викладачів за заданою умовою та збереження результатів у CSV. 
    Призначення: автоматизує виділення груп викладачів (наприклад, найкращих або найгірших
    за показниками) для подальшого аналізу.
    
    Вхідні параметри: 
    - df (pandas.DataFrame) — датафрейм з аналітичними даними про викладачів.
    - condition (bool mask) — умова фільтрації рядків.
    - filename (str) — шлях до CSV-файлу для збереження результатів.

    Вихід: список імен викладачів, що відповідають умові.
    """
    group = df[condition]
    group["name"].to_csv(filename, index=False)
    return group["name"].to_list()


def extract_teachers_from_db(db_path, query, output_csv):
    """
    Виконує SQL-запит до SQLite-бази даних, завантажує результат у pandas DataFrame
    та зберігає його у CSV-файл.

    Параметри
    ----------
    db_path : str
        Шлях до файлу SQLite-бази даних.
    query : str
        SQL-запит, який потрібно виконати (наприклад, JOIN викладачів з їхніми показниками).
    output_csv : str
        Шлях до CSV-файлу, у який буде збережено результат.

    Повертає
    -------
    pandas.DataFrame
        DataFrame з результатом виконання SQL-запиту.

    Функціональність
    ----------------
    1. Встановлює з'єднання з базою SQLite.
    2. Виконує SQL-запит і завантажує дані у pandas DataFrame.
    3. Закриває з'єднання з базою.
    4. Експортує результат у CSV з кодуванням UTF-8 (для сумісності з Excel).
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    return df


def prepare_loss_data(df_analysis, df_gone):
    """
    Об’єднує аналітичні дані викладачів із даними про відвали учнів.

    Параметри
    ----------
    df_analysis : pandas.DataFrame
        Датафрейм із аналітичними показниками викладачів (KPI)
        (містить стовпці 'teacher_name' і 'Date').
    df_gone : pandas.DataFrame
        Датафрейм із даними про відвали учнів 
        (містить стовпці 'name_normalized', 'start_date', 'attendance_number').

    Повертає
    -------
    pandas.DataFrame
        Об’єднаний датафрейм, де для кожного викладача та місяця додано:
        - 'loss_number' — кількість відвалів (учнів, що залишили заняття).

    Функціональність
    ----------------
    1. Конвертує дати в обох датафреймах у формат datetime.
    2. Створює новий стовпець 'Month' (тип Period) для зручного групування по місяцях.
    3. Підраховує кількість відвалів ('attendance_number') по кожному викладачу та місяцю.
    4. Перейменовує стовпці для узгодження форматів (name_normalized → teacher_name).
    5. Виконує left-join аналітичних даних із кількістю відвалів по викладачах.

    """
    # Конвертація дат
    for df, col in [(df_analysis, "Date"), (df_gone, "start_date")]:
        df[col] = pd.to_datetime(df[col])
        df["Month"] = df[col].dt.to_period("M")

    # Кількість відвалів по викладачу/місяцю
    loss_counts = (
        df_gone.groupby(["name_normalized", "Month"])["attendance_number"]
        .count()
        .reset_index()
        .rename(columns={"name_normalized": "teacher_name", "attendance_number": "loss_number"})
    )

    # Мердж
    return df_analysis.merge(loss_counts, on=["teacher_name", "Month"], how="left")

def calculate_work_period(df):
    """
    Обчислює період роботи викладачів у місяцях на основі їхніх даних.

    Параметри
    ----------
    df : pandas.DataFrame
        Вхідний датафрейм, що містить щонайменше стовпці:
        - 'teacher_name' : ім’я викладача
        - 'Month' : період роботи (тип pandas.Period[M])

    Повертає
    -------
    pandas.DataFrame
        Датафрейм з такими стовпцями:
        - 'teacher_name' : ім’я викладача
        - 'min' : перший місяць роботи
        - 'max' : останній місяць роботи
        - 'months_worked' : загальна кількість місяців роботи
    """
    work_period = df.groupby("teacher_name")["Month"].agg(["min", "max"]).reset_index()
    work_period["months_worked"] = (
        (work_period["max"] - work_period["min"]).apply(lambda x: x.n) + 1
    )
    return work_period


def add_quality_importance(df):
    """
    Розраховує питомі ваги показників якості викладачів в загальної кількості KPI, а також частку
    кожного KPI в ставшихся відвалах.

    Параметри
    ----------
    df : pandas.DataFrame
        Датафрейм із метриками якості викладачів. 
        Містить:
        - стовпці з показниками якості (визначаються у QUALITY_COLS),
        - стовпець 'loss_number_normalized' для нормалізованої кількості відвалів.

    Повертає
    -------
    pandas.DataFrame
        Той самий датафрейм із доданими стовпцями:
        - 'p' : питомі ваги кожного показника якості в загальній їх сумі,
        - 'importance_of_<short>' : частки кожного показника якості в кількості нормалізованих 
           відвалів (помісячно).

    Функціональність
    ----------------
    1. Нормалізує значення метрик якості (ділить на 100).
    2. Обчислює показник `p` як обернену суму коефіцієнтів якості для викладача (питомі ваги 
       кожного показника якості в загальній їх сумі).
    3. Для кожного показника якості додає стовпець `importance_of_<short>`, 
       де `<short>` — скорочене позначення показника з QUALITY_SHORT. Importance - це частка
       кожного KPI у відвалах, розрахованих помісячно.
    4. Повертає датафрейм з оновленими метриками.

    """
    # Нормалізація у коефіцієнти
    for col in QUALITY_COLS:
        df[col] = df[col] / 100

    # Показник p
    df["p"] = round(1 / df[QUALITY_COLS].sum(axis=1), 2)

    # Важливість кожного показника
    for col, short in QUALITY_SHORT.items():
        df[f"importance_of_{short}"] = round(df["p"] * df[col] * df["loss_number_normalized"], 2)
    return df


# ======================
# Main pipeline
# ======================
def main():
    # --- 1. Завантаження початкових даних ---
    df_analysis = pd.read_csv("data/teachers_analysis.csv")

    # --- 2. Групи викладачів (A, B, C, D) ---
    # A, C, D
    for group, condition in TEACHER_GROUPS.items():
        if condition is not None:
            save_teacher_group(df_analysis, condition(df_analysis), f"data/teachers_{group}.csv")

    # B — викладачі у двох різних категоріях
    mask = False
    for col1, col2 in combinations(CATEGORY_COLS, 2):
        mask |= (df_analysis[col1] == 1) & (df_analysis[col2] == 1)
    save_teacher_group(df_analysis, mask, "data/teachers_B.csv")

    # --- 3. Витяг з бази даних ---
    query = """
    SELECT 
        t.Teacher AS teacher_name,
        ts.Date,
        ts.Feedback_for_parents,
        ts.Feedback_to_students,
        ts.Control_of_homework,
        ts.Control_of_potential_loss,
        ts.Average_success
    FROM TeacherStats ts
    JOIN Teachers t ON ts.Teacher = t.Teacher
    ORDER BY t.Teacher, ts.Date;
    """
    df_teachers_rate = extract_teachers_from_db(
        "data/teacher_analysis.sqlite", query, "data/Analysis_teachers_rate.csv"
    )

    # --- 4. Додавання даних по відвалах ---
    df_gone = pd.read_csv("data/General_Gone_Clients_Report.csv")
    df_merged = prepare_loss_data(df_teachers_rate, df_gone)

    # --- 5. Обчислення кількості місяців роботи ---
    work_period = calculate_work_period(df_merged)
    df_merged = df_merged.merge(work_period[["teacher_name", "months_worked"]], on="teacher_name", how="left")

    # --- 6. Підрахунок сумарних відвалів ---
    loss_sum = (
        df_merged.groupby("teacher_name")["loss_number"]
        .sum()
        .reset_index()
        .rename(columns={"loss_number": "total_loss_number"})
    )
    df_merged = df_merged.merge(loss_sum, on="teacher_name", how="left")

    # --- 7. Нормалізація по часу відвалів по кожному викладачу (один період часу - місяць) ---
    df_merged["loss_number_normalized"] = round(
        df_merged["total_loss_number"] / df_merged["months_worked"], 2
    )
    df_merged["loss_number"] = df_merged["loss_number"].fillna(0).astype(int)

    # --- 8. Розрахунок питомої ваги ---
    df_final = add_quality_importance(df_merged)

    # --- 9. Збереження результату ---
    df_final.to_csv("data/Analysis_teacher_with_importance.csv", index=False)

    print("✅ Аналіз завершено! Результати збережено у Analysis_teacher_with_importance.csv")


if __name__ == "__main__":
    main()