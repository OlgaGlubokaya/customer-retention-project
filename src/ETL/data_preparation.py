"""
Назва файлу: data_preparation.py
Мета: додати в головний датасет для аналізу дані стосовно вартості курсів, заробітньої
плати викладачів, фінансові дані стосовно втрат викладачів і школи від кожного втраченого студента
(з врахуванням кількості занять курсу, що він у нас купив).

Основні кроки:
-----------------------------------
1. Завантаження базового CSV з відвалами клієнтів.
2. Додавання колонок з вартістю курсів і зарплатами (на основі міста та дати).
3. Перетворення дат у формат datetime з перевіркою на валідність.
4. Feature engineering:
   - teacher_lost: втрати викладача від одного відвалу.
   - school_lost: втрати школи від одного відвалу.
5. Збереження фінальних результатів у CSV.

Вхідні файли:
- data/Final_Gone_Clients_Report.csv
- data/costs_salaries_converted.csv

Вихідні файли:
- data/General_Gone_Clients_Report.csv

"""

import pandas as pd
import logging

# ----------------------- CONFIGURATION -----------------------
INPUT_FILE = "data/Final_Gone_Clients_Report.csv"
COSTS_FILE = "data/costs_salaries_converted.csv"
OUTPUT_FILE_GENERAL = "data/General_Gone_Clients_Report.csv"

ALL_LESSONS = 40
ALL_MONTHS = 9
AVG_STUDENTS_PER_GROUP = 10

# Періоди з відповідними колонками для вартості й зарплати
PERIODS = {
    ("2022-01-01", "2023-07-31"): ("2022_cost", "2022_salary"),
    ("2023-08-01", "2024-08-31"): ("2023_cost", "2023_salary"),
}

# ----------------------- LOGGING -----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d",
)

# ----------------------- FUNCTIONS -----------------------
def load_data(file_path: str) -> pd.DataFrame:
    """
    Завантажує CSV-файл у DataFrame.

    Параметри:
    ----------
    file_path : str
        Шлях до CSV-файлу, який потрібно завантажити.

    Повертає:
    -------
    pd.DataFrame
        Pandas DataFrame з даними з файлу.

    Викидає:
    -------
    Exception
        Піднімає помилку, якщо файл не знайдено або неможливо прочитати CSV.
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Файл {file_path} успішно завантажено. Рядків: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Помилка завантаження файлу {file_path}: {e}")
        raise


def preprocess_dates(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Перетворює колонку у формат datetime та перевіряє на некоректні значення.

    Параметри:
    ----------
    df : pd.DataFrame
        DataFrame, який містить колонку для обробки.
    column : str
        Назва колонки, яка буде конвертована у datetime.

    Повертає:
    -------
    pd.DataFrame
        DataFrame з перетвореною колонкою. Некоректні значення замінюються на NaT.

    Додатково:
    ----------
    Якщо є некоректні або порожні значення у колонці, функція 
    логуватиме попередження через logging.warning, але не зупиняє виконання.
    """
    df[column] = pd.to_datetime(df[column], errors="coerce")
    if df[column].isnull().any():
        logging.warning(f"Знайдено некоректні значення в {column}")
    return df


def get_costs_and_salaries(row, costs_df: pd.DataFrame) -> pd.Series:
    """
    Повертає вартість курсу та зарплату викладача на основі міста та дати початку курсу.

    Функція використовує словник PERIODS для визначення періоду (рік/місяць) 
    і відповідних колонок з вартостями та зарплатами у costs_df.

    Параметри:
    ----------
    row : pd.Series
        Рядок DataFrame, який містить колонки "city" та "start_date".
    costs_df : pd.DataFrame
        DataFrame з інформацією про вартість курсів та зарплати викладачів по містах 
        та роках. Містить колонку "city" та колонки відповідних періодів 
        (наприклад, "2022_cost", "2022_salary", "2023_cost", "2023_salary").

    Повертає:
    -------
    pd.Series
        Серія з двома значеннями:
        - course_cost_in_month : float | None
        - teacher_salaries : float | None
        Якщо місто не знайдено або дата не належить жодному періоду — повертає [None, None].

    Додатково:
    ----------
    Функція передбачає, що дата у колонці "start_date" вже конвертована у datetime.
    """
    city, date = row["city"], row["start_date"]
    values = costs_df.loc[costs_df["city"] == city]

    if values.empty or pd.isna(date):
        return pd.Series([None, None])

    values = values.iloc[0]
    for (start, end), (cost_col, sal_col) in PERIODS.items():
        if pd.Timestamp(start) <= date <= pd.Timestamp(end):
            return pd.Series([values[cost_col], values[sal_col]])

    return pd.Series([None, None])


def add_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Додає колонки з інженерією ознак (feature engineering) для оцінки втрат.

    Функція створює дві нові колонки:
    - teacher_lost : фінансові втрати викладача через пропуски занять студентами.
    - school_lost : фінансові втрати школи через пропуски занять студентами.

    Параметри:
    ----------
    df : pd.DataFrame
        DataFrame, який містить колонки:
        - "teacher_salaries" : зарплата викладача за місяць
        - "course_cost_in_month" : вартість курсу за місяць
        - "attendance_number" : кількість відвіданих занять студентом

    Повертає:
    -------
    pd.DataFrame
        DataFrame з доданими колонками "teacher_lost" та "school_lost".

    Примітки:
    ----------
    Використовуються глобальні константи:
    - ALL_LESSONS : загальна кількість уроків у серії
    - ALL_MONTHS : кількість місяців курсу
    """
    df["teacher_lost"] = df["teacher_salaries"] * (ALL_LESSONS - df["attendance_number"])
    df["school_lost"] = (
        df["course_cost_in_month"] * ALL_MONTHS / ALL_LESSONS * (ALL_LESSONS - df["attendance_number"])
    )
    return df


def save_data(df: pd.DataFrame, file_path: str) -> None:
    """
    Зберігає DataFrame у CSV-файл.

    Функція записує DataFrame у вказаний файл у форматі CSV без індексів 
    і логуватиме інформацію про успішне збереження.

    Параметри:
    ----------
    df : pd.DataFrame
        DataFrame, який потрібно зберегти.
    file_path : str
        Шлях до CSV-файлу, куди буде здійснено запис. Може включати назву файлу і директорію.

    Повертає:
    -------
    None
        Функція не повертає значення, лише виконує запис у файл і логування.

    Додатково:
    ----------
    При успішному збереженні у лог буде записано повідомлення INFO з шляхом файлу.
    """
    df.to_csv(file_path, index=False)
    logging.info(f"Файл збережено: {file_path}")


# ----------------------- MAIN SCRIPT -----------------------
if __name__ == "__main__":
    # 1. Завантаження даних
    df = load_data(INPUT_FILE)
    costs_df = load_data(COSTS_FILE)

    # 2. Попередня обробка дат
    df = preprocess_dates(df, "start_date")

    # 3. Додаємо нові стовпці з None (float dtype)
    df["course_cost_in_month"] = pd.Series(dtype="float")
    df["teacher_salaries"] = pd.Series(dtype="float")

    # 4. Заповнюємо вартості курсів і зарплати
    df[["course_cost_in_month", "teacher_salaries"]] = df.apply(
        get_costs_and_salaries, axis=1, costs_df=costs_df
    )

    # 5. Feature engineering
    df = add_features(df)

    # 6. Збереження фінального результату
    save_data(df, OUTPUT_FILE_GENERAL)

    logging.info("Обробка завершена успішно ✅")

