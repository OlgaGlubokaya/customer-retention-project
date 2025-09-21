"""
Назва файлу: data_io.py

Мета:
Завантаження та збереження даних у форматі CSV для ETL-процесів
та підготовки даних до подальшого аналізу.

Призначення модуля
------------------
Модуль містить функції для роботи з CSV-файлами:

1. `load_data(file_path: str) -> pd.DataFrame`
   - Завантажує CSV-файл у pandas DataFrame.
   - Логує успішне завантаження та кількість рядків.
   - У разі помилки повідомляє про проблему та піднімає виняток.

2. `save_data(df: pd.DataFrame, file_path: str) -> None`
   - Зберігає DataFrame у CSV-файл без індексу.
   - Логує успішне збереження файлу.

Призначення:
- Використовується у ETL-пайплайнах для завантаження сирих даних та збереження проміжних або оброблених результатів.
- Забезпечує прозоре логування процесу роботи з файлами.
"""


import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_data(file_path: str) -> pd.DataFrame:
    """
    Функція для завантаження CSV-файлу у pandas DataFrame.

    Аргументи:
    - file_path: шлях до CSV-файлу (str)

    Функціонал:
    1. Читає CSV-файл у DataFrame.
    2. Логує успішне завантаження та кількість рядків.
    3. У разі помилки логування повідомляє про проблему та повторно піднімає виняток.
"""

    try:
        df = pd.read_csv(file_path)
        logging.info(f"Файл {file_path} успішно завантажено. Рядків: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Помилка завантаження файлу {file_path}: {e}")
        raise

def save_data(df: pd.DataFrame, file_path: str) -> None:
    """
    Функція для збереження pandas DataFrame у CSV-файл.

    Аргументи:
    - df: pandas DataFrame з даними
    - file_path: шлях до CSV-файлу, куди зберігати дані (str)

    Функціонал:
    1. Зберігає DataFrame у CSV без індексу.
    2. Логує успішне збереження файлу.
"""

    df.to_csv(file_path, index=False)
    logging.info(f"Файл збережено: {file_path}")
