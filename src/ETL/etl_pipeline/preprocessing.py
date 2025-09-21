"""
Назва файлу: preprocessing.py

Мета:
Попередня обробка та нормалізація даних для ETL-процесів,
зокрема робота з датами, іменами викладачів та віковими категоріями.

Призначення модуля
------------------
Модуль містить функції для очищення та підготовки даних у DataFrame:

1. `preprocess_dates(df: pd.DataFrame, column: str) -> pd.DataFrame`
   - Обробка колонок з датами: конвертація у datetime, попередження про некоректні значення.

2. `normalize_teacher(name: str) -> str`
   - Нормалізація імен викладачів: видалення зайвих пробілів та виділення перших двох слів.

3. `map_teacher_names(df: pd.DataFrame, name_map: dict) -> pd.DataFrame`
   - Відображення імен викладачів за словником для уніфікації назв.

4. `categorize_age(df: pd.DataFrame, age_column: str) -> pd.DataFrame`
   - Категоризація віку учнів у групи A–D за визначеними віковими інтервалами.

Призначення:
- Використовується у ETL-пайплайнах для підготовки даних до аналізу.
- Забезпечує стандартизацію даних та логування ключових моментів обробки.
"""
import pandas as pd
import re
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def preprocess_dates(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Функція для попередньої обробки колонок з датами у DataFrame.

    Аргументи:
    - df: pandas DataFrame з даними
    - column: назва колонки з датами (str)

    Функціонал:
    1. Перетворює значення колонки у формат datetime.
    2. Некоректні значення конвертуються у NaT.
    3. Якщо в колонці є некоректні дати, виводиться попереджувальне повідомлення через logging.
    4. Повертає DataFrame з обробленою колонкою.
"""
    # копія для перевірки валідності
    original = df[column].copy()

    df[column] = pd.to_datetime(df[column], errors="coerce")
    df[f"{column}_invalid"] = df[column].isna() & original.notna()

    invalid_count = df[f"{column}_invalid"].sum()
    if invalid_count > 0:
        logging.warning(f"У колонці {column} знайдено {invalid_count} некоректних дат")

    return df

 
def normalize_teacher(name: str) -> str:
    """
    Функція для нормалізації імені викладача.

    Аргументи:
    - name: рядок з ім’ям викладача (str)

    Функціонал:
    1. Видаляє пробіли на початку та в кінці рядка.
    2. Витягує перші два слова (ім’я та прізвище) за допомогою регулярного виразу.
    3. Якщо відповідність знайдена, повертає нормалізоване ім’я; інакше — повертає оригінальний рядок.
"""

    name = name.strip()
    match = re.match(r"^([\wА-Яа-яІіЇїЄєҐґ']+\s[\wА-Яа-яІіЇїЄєҐґ']+)", name)
    return match.group(1) if match else name

def map_teacher_names(df: pd.DataFrame, name_map: dict) -> pd.DataFrame:
    """
    Функція для відображення (мапінгу) імен викладачів за словником.

    Аргументи:
    - df: pandas DataFrame з даними
    - name_map: словник {оригінальне ім’я: нормалізоване ім’я}

    Функціонал:
    1. Створює нову колонку 'name_normalized'.
    2. Для кожного значення в 'teacher_normalized' застосовує словник name_map.
    3. Якщо значення знайдено в словнику, повертає нормалізоване ім’я; інакше залишає оригінал.
    4. Повертає DataFrame з новою колонкою.
"""

    df['name_normalized'] = df['teacher_normalized'].apply(lambda x: name_map.get(x.strip(), x.strip()))
    return df

def categorize_age(df: pd.DataFrame, age_column: str) -> pd.DataFrame:
    """
    Функція для категоризації віку учнів за групами.

    Аргументи:
    - df: pandas DataFrame з даними
    - age_column: назва колонки з віком (str)

    Функціонал:
    1. Визначає вікові інтервали (bins) та відповідні мітки груп (labels).
    2. Створює нову колонку 'age_group', у якій кожен учень отримує категорію A–D відповідно до віку.
    3. Повертає DataFrame з доданою колонкою 'age_group'.
"""

    bins = [6, 8, 10, 13, 17]
    labels = ["A", "B", "C", "D"]

    df["age_group"] = pd.cut(df[age_column], bins=bins, labels=labels, include_lowest=True)

    # прапорець для віку поза межами інтервалів
    df["out_of_range_age"] = df["age_group"].isna() & df[age_column].notna()

    out_of_range_count = df["out_of_range_age"].sum()
    if out_of_range_count > 0:
        logging.warning(f"Учнів з віком поза інтервалами: {out_of_range_count}")

    return df
    