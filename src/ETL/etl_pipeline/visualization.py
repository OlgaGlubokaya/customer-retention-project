"""
Назва файлу: visualization.py

Мета:
Візуалізація даних у форматі стовпчикових графіків та boxplot для аналітики
в освітніх ETL-процесах та оцінки показників викладачів.

Призначення модуля
------------------
Модуль містить функції для побудови графіків з DataFrame та маркування ключових значень:

1. `plot_pie(df, labels_col, values_col, title, file_name)`
   - Створює кругову діаграму з розподілом за групами (hue).
   - Додає підписи числових значень.
   - Оптимізує макет діаграми та зберігає її у файл з високою роздільною здатністю.
   - Відображає діаграму на екрані.

2. `plot_box(df, column, title, file_name)`
   - Створює boxplot для обраної колонки.
   - Обчислює викиди (значення понад Q3 + 1.5*IQR) та позначає їх на графіку іменами викладачів.
   - Налаштовує заголовок, сітку та макет графіка.
   - Зберігає графік у файл та відображає його на екрані.
   - Повертає список імен викладачів, що є викидами.

Призначення:
- Використовується для швидкої та наочної оцінки даних.
- Підтримує стандартизоване збереження графіків та підписів ключових точок.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[3]  # піднятися на 3 рівні вгору
IMAGES_DIR = PROJECT_ROOT / "image"

def plot_pie(df, labels_col, values_col, title, file_name, wrap_len=20):
    """
    Функція для побудови кругової діаграми (pie chart).

    Аргументи:
    - df: pandas DataFrame з даними для візуалізації
    - labels_col: назва колонки для підписів секторів (str)
    - values_col: назва колонки з числовими значеннями (str)
    - title: заголовок графіка (str)
    - file_name: шлях та назва файлу для збереження графіка (str)

    Функціонал:
    1. Створює кругову діаграму.
    2. Відображає підписи секторів та відсоткові значення.
    3. Налаштовує заголовок графіка.
    4. Зберігає графік у файл з високою роздільною здатністю (dpi=300).
    5. Відображає графік на екрані.
    """
    import textwrap
    
    # Робимо перенос тексту у підписах
    labels = [ "\n".join(textwrap.wrap(str(label), wrap_len)) for label in df[labels_col] ]
    
    plt.figure(figsize=(8, 8))
    plt.pie(
        df[values_col],
        labels=labels,
        autopct='%1.1f%%',
        startangle=90,
        colors=sns.color_palette("Set2", n_colors=len(df))
    )
    plt.title(title)
    plt.tight_layout()
    plt.savefig(file_name, dpi=300)
    plt.show()
    
def plot_box_compare(df1, df2, value_column, teacher_column, labels, title, file_name):
    """
    Побудова boxplot для порівняння двох періодів на одному графіку.

    Аргументи:
    - df1: DataFrame для першого періоду
    - df2: DataFrame для другого періоду
    - column: назва колонки для побудови boxplot
    - labels: список назв категорій для осі X (["2022/2023", "2023/2024"])
    - title: заголовок графіка
    - file_name: шлях та назва файлу для збереження графіка

    Повертає:
    - список викладачів, яки є викидами і мають найбільші або найменьші відсотки відвалів так, що
    виділяються серед усіх і розраховуються по фомулам:
        - lower_bound = q1 - 1.5 * iqr
        - upper_bound = q3 + 1.5 * iqr 
        де iqr = q3 - q1
    """
    
    # підготовка даних
    df1 = df1[[value_column, teacher_column]].copy()
    df1["Рік"] = labels[0]

    df2 = df2[[value_column, teacher_column]].copy()
    df2["Рік"] = labels[1]

    combined = pd.concat([df1, df2], axis=0)

    plt.figure(figsize=(10, 6))
    ax = sns.boxplot(
        x="Рік",
        y=value_column,
        hue="Рік",
        data=combined,
        palette="Set2",
        fliersize=4,
        legend=False
    )

    # список викидів (тільки прізвища)
    outliers = []

    # підписуємо викиди прізвищами
    for i, year in enumerate(labels):
        subset = combined[combined["Рік"] == year]
        values = subset[value_column].values
        teachers = subset[teacher_column].values

        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        for y, teacher in zip(values, teachers):
            if y < lower_bound or y > upper_bound:
                ax.text(i, y, teacher, horizontalalignment='center', color='red', fontsize=9, rotation=30)
                outliers.append(teacher)

    plt.title(title)
    plt.xlabel("Навчальний рік")
    plt.ylabel("% відвалів на одну групу")
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(file_name, dpi=300)
    plt.show()

    return outliers

    


    