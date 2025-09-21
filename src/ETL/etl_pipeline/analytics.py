"""
Назва файлу: analytics.py

Мета:
Аналіз втрат учнів за період та формування аналітичних даних для подальшого звітування
та оцінки роботи викладачів.

Призначення модуля
------------------
Модуль містить функції для обробки даних про відвідуваність учнів та визначення
груп викладачів за рівнем втрат:

1. `get_loss_dataframe(dframe: pd.DataFrame, start_date: str, finish_date: str,
   name_from_csv_file: str, name_to_csv_file: str) -> pd.DataFrame`
   - Формує аналітичний DataFrame щодо втрат учнів за вказаний період.
   - Підраховує глобальний та груповий відсоток втрат.
   - Зберігає результати у CSV та логує успішне збереження.

2. `get_quartile_lists(df: pd.DataFrame, Q1=0.25, Q2=0.5, Q3=0.75) -> dict`
   - Розраховує квартилі показника 'percent_of_loss_for_one_group'.
   - Формує три списки викладачів:
     - "best": найменші втрати
     - "interquartile": середні втрати
     - "bad": найбільші втрати
   - Повертає словник з категоризованими списками викладачів.

Призначення:
- Використовується у ETL-пайплайнах та аналітичних звітах для оцінки роботи викладачів.
- Забезпечує стандартизацію та збереження аналітичних даних з логуванням ключових етапів.
"""

import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_loss_dataframe(dframe: pd.DataFrame, start_date: str, finish_date: str,
                       name_from_csv_file: str, name_to_csv_file: str) -> pd.DataFrame:
    """
    Функція для формування аналітичного DataFrame щодо втрат учнів за період.

    Аргументи:
    - dframe: pandas DataFrame з даними про активність учнів
    - start_date: початкова дата періоду у форматі 'YYYY-MM-DD' (str)
    - finish_date: кінцева дата періоду у форматі 'YYYY-MM-DD' (str)
    - name_from_csv_file: шлях до CSV-файлу з інформацією про групи та кількість учнів (str)
    - name_to_csv_file: шлях до CSV-файлу для збереження результатів (str)

    Функціонал:
    1. Відбирає рядки DataFrame, які потрапляють у вказаний період.
    2. Групує дані за нормалізованими іменами викладачів та сумує показник 'count'.
    3. З’єднує отриману статистику з даними про групи та кількість учнів із зовнішнього CSV.
    4. Видаляє колонку 'count_of_loss', якщо вона існує.
    5. Розраховує:
       - глобальний відсоток втрат по кожному викладачу ('global_percent_of_loss')
       - відсоток втрат на одну групу ('percent_of_loss_for_one_group')
    6. Зберігає готовий DataFrame у CSV.
    7. Логує інформацію про успішне збереження файлу.
    8. Повертає оброблений DataFrame з аналітикою.
    """
    mask = (dframe["start_date"] >= start_date) & (dframe["start_date"] <= finish_date)
    df_period = dframe[mask].copy()
    
    stats = df_period.groupby("name_normalized", as_index=False)['count'].sum()
    
    groups_and_losts = pd.read_csv(name_from_csv_file)
    stats = pd.merge(stats, groups_and_losts, on="name_normalized", how="left")
    stats = stats.drop(columns=['count_of_loss'])

# Створимо декілька додаткових стовпців, необхідних для аналізу даних:
# -  "global_percent_of_loss" - загальний відсоток відвалів викладача за навчальний рік:  
    stats["global_percent_of_loss"] = round(stats["count"]*100/stats["number_of_students"], 2)
    stats["percent_of_loss_for_one_group"] = round(stats["global_percent_of_loss"]/stats["number_of_group"], 2)

# Зберігаємо в окремий файл для подальшої роботи при математичному моделюванні
    stats.to_csv(name_to_csv_file, index=False)
    logging.info(f"Аналітичний файл збережено: {name_to_csv_file}")
    
    return stats

def get_quartile_category_teachers_list(df: pd.DataFrame, Q1=0.25, Q2=0.5, Q3=0.75):
    """
    Функція для формування списків викладачів за квартилями відсотка втрат учнів.

    Аргументи:
    - df: pandas DataFrame з колонкою 'percent_of_loss_for_one_group' та 'name_normalized'
    - Q1: перший квартиль (float, за замовчуванням 0.25)
    - Q2: медіана/другий квартиль (float, за замовчуванням 0.5)
    - Q3: третій квартиль (float, за замовчуванням 0.75)

    Функціонал:
    1. Розраховує значення квартилів для показника 'percent_of_loss_for_one_group'.
    2. Формує три категорії викладачів:
       - "best": викладачі з показником менше Q1 (найменші втрати)
       - "interquartile": викладачі з показником між Q1 та Q3 (середні втрати)
       - "bad": викладачі з показником більше Q3 (найбільші втрати)
    3. Повертає словник з трьома категоріями і списком викладачів в кожній з них.
    """    
    def list_for_indicator(df, indicator):
        q = df["percent_of_loss_for_one_group"].quantile(indicator)
        if indicator == Q1:
            return df[df["percent_of_loss_for_one_group"] < q]["name_normalized"].tolist()
        elif indicator == Q3:
            return df[df["percent_of_loss_for_one_group"] > q]["name_normalized"].tolist()
        else:
            iq1 = df["percent_of_loss_for_one_group"].quantile(Q1)
            iq3 = df["percent_of_loss_for_one_group"].quantile(Q3)
            return df[(df["percent_of_loss_for_one_group"] >= iq1) & (df["percent_of_loss_for_one_group"] <= iq3)]["name_normalized"].tolist()
    
    return {
        "best": list_for_indicator(df, Q1),
        "interquartile": list_for_indicator(df, Q2),
        "bad": list_for_indicator(df, Q3)
    }
