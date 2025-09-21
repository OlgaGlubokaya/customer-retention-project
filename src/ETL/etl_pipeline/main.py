"""
Назва файлу: main.py

Мета:
Основний модуль ETL-проєкту для аналізу відвалів студентів у навчальних групах. Створений для:
- категоризації віку студентів, які пішли з курсу
- визначення груп викладачів за кількістю їх відвалів (визначені Key points - 
викладачі з найвищим, середним і найнижчим відвалом, постійні “проблемні” викладачі)
- визначення і візуалізації топ-3 причин відвалів по вікових категоріях (Findings: 
топ-3 причин відтоку по кожній віковій групі, викладачі у викидах)
- знаходження insight для визначення подальшого напрямку роботи з даними

Функціонал:
-----------
1. Читання даних із CSV.
2. Попередня обробка:
   - нормалізація та виправлення імен викладачів,
   - перетворення дат,
   - категоризація віку студентів.
3. Фільтрація датасету за навчальні роки (2022–2023 та 2023–2024).
4. Збереження очищених даних для подальшої обробки.
5. Формування статистики по викладачах за два навчальні роки.
6. Візуалізація:
   - boxplot із відсотком відвалів на одну групу,
   - plot_pie із топ-3 причин відвалу по вікових групах: 4 категорії.
7. Квартильний аналіз викладачів по показнику "річний відсоток відвалів вкладача в розрахунку
   на одну його групу" для виділення наступних категорій викладачів:
   - «кращих»,
   - «середніх»,
   - «проблемних» викладачів.
8. Вивід результатів у CSV та графічні файли.

Призначення:
------------
Скрипт використовується як головна точка запуску модуля etl-pipeline:
від завантаження та очищення даних до отримання готової статистики,
візуалізацій і фінальних списків викладачів за рівнем ефективності.

Вхідні дані:
------------
- data/General_Gone_Clients_Report.csv — датасет з даними про студентів, що припинили навчання,
доповнений фінансовими даними стосовно втрат школи і викладачів, за період з 01.01.2022 по 31.07.2024.
- data/groups_and_losts_2022_2023.csv — додаткові дані по групах і відвалах за 2022/2023, які містять
кількість викладачів, кількість відвалів на одного викладача, кількість груп викладача, загальну 
кількість студентів у кожного викладача.
- data/groups_and_losts_2023_2024.csv — додаткові дані по групах і відвалах за 2023/2024, які містять
кількість викладачів, кількість відвалів на одного викладача, кількість груп викладача, загальну 
кількість студентів у кожного викладача.

Вихідні дані: 
-------------
- Оновлений датасет із фільтрацією за періодами (CSV) - data/General_Gone_Clients_Report.csv
- Аналітичні таблиці по викладачах за кожен рік (CSV) - data/Teachers_Data_analysis_2022_2023.csv,
data/Teachers_Data_analysis_2023_2024.csv
- Графік розподілу відвалів (PNG) - images/boxplot_comparison.png.
- Топ-3 причин відвалу по вікових групах (CSV і PNG):
    images/top_lost_reasons_by_age_group_A.png (7 - 8 років)
    data/Top3_reasons_of_loss_A.csv

    images/top_lost_reasons_by_age_group_B.png (9 - 10 років)
    data/Top3_reasons_of_loss_B.csv

    images/top_lost_reasons_by_age_group_C.png (11 - 12 років)
    data/Top3_reasons_of_loss_C.csv

    images/top_lost_reasons_by_age_group_D.png (13 - 14 років)
    data/Top3_reasons_of_loss_D.csv

- Список викладачів по усім чотирьом категоріяи - "data/teachers_analysis.csv".

"""

from src.ETL.etl_pipeline import data_io, preprocessing, analytics, visualization
from collections import Counter
import pandas as pd

if __name__ == "__main__":

    # --------------------------
    # 1️⃣ Читання даних
    # --------------------------
    df = data_io.load_data("data/General_Gone_Clients_Report.csv")

    # --------------------------
    # 2️⃣ Підготовка даних
    # --------------------------
    df = preprocessing.preprocess_dates(df, "start_date")
    
    # Видаляємо зайві пробіли та нормалізуємо імена викладачів
    df["teacher_name"] = df["teacher_name"].str.strip()
    # Створюємо стовпець "count" для внесення туди повторів викладачей (тобто таким чином ми рахуємо
#  кількість відвалів на кожного викладача)
    df["count"]=1
    df["teacher_normalized"] = df["teacher_name"].apply(preprocessing.normalize_teacher)

    # Словник для виправлення імен викладачів
    name_map = {
       "Кнідзе Міша": "Світлозар Чаус",
       "Книдзе Михаил": "Світлозар Чаус",
       "Сазонова Ліза": "Сазонова Єлизавета",
       "Котельніков Сергій": "Котельников Сергій",
       "Котельніков 50": "Котельников Сергій",
       "Гончарук Онлайн": "Гончарук Денис",
       "Удодік Іра": "Удодік Ірина",
       "Олег Попруга": "Попруга Олег",
       "Клименко 250": "Клименко Владислав",
    }
    df = preprocessing.map_teacher_names(df, name_map)

    # Створюємо вікові групи
    df = preprocessing.categorize_age(df, "age")

    # --------------------------
    # 3️⃣ Фільтруємо потрібний період
    # --------------------------
    df_filtered = df[(df["start_date"] >= "2022-08-01") & (df["start_date"] <= "2024-07-31")].copy()

    # --------------------------
    # 4️⃣ Зберігаємо в основний датасет для ETL
    # --------------------------
    data_io.save_data(df_filtered, "data/General_Gone_Clients_Report.csv")

    # --------------------------
    # 5️⃣ Статистика по викладачах за два роки
    # --------------------------
    stats_22_23 = analytics.get_loss_dataframe(
        df_filtered,
        start_date="2022-08-01",
        finish_date="2023-07-31",
        name_from_csv_file="data/groups_and_losts_2022_2023.csv",
        name_to_csv_file="data/Teachers_Data_analysis_2022_2023.csv"
    )

    stats_23_24 = analytics.get_loss_dataframe(
        df_filtered,
        start_date="2023-08-01",
        finish_date="2024-07-31",
        name_from_csv_file="data/groups_and_losts_2023_2024.csv",
        name_to_csv_file="data/Teachers_Data_analysis_2023_2024.csv"
    )

    # --------------------------
    # 6️⃣ Візуалізація порівняння відсотка відвалів викладачів за два роки
    # --------------------------

    outliers_teachers_all = visualization.plot_box_compare(stats_22_23, stats_23_24, 
                                    value_column="percent_of_loss_for_one_group",
                                    teacher_column ="name_normalized",
                                    labels=["2022/2023", "2023/2024"], 
                                    title = "Розподіл річного відсотка відвалів по викладачам в розрахунку на одну групу", 
                                    file_name ="images/boxplot_comparison.png")


    # --------------------------
    # 7️⃣ Топ-3 причин відвалу по вікових групах A, B, C, D
    # --------------------------

    age_groups = {
        "A": "7-8 years",
        "B": "9-10 years",
        "C": "11-12 years",
        "D": "13-14 years"
    }

    for group, label in age_groups.items():
        # Фільтруємо по групі
        reason_counts = (
            df[df["age_group"] == group]
            ["lost_reasons"]
            .value_counts()
            .nlargest(3)
            .reset_index(name="count")
        )
        reason_counts.rename(columns={"index": "lost_reasons"}, inplace=True)

        # Зберігаємо CSV (опційно, можна зберегти один файл для всіх груп, тоді append)
        data_io.save_data(reason_counts, f"data/Top3_reasons_of_loss_{group}.csv")

        # Будуємо pie chart
        visualization.plot_pie(
            reason_counts,
            labels_col="lost_reasons",
            values_col="count",
            title=f"Top 3 reasons for the loss of clients in age group {label}",
            file_name=f"images/top_lost_reasons_by_age_group_{group}.png"
        )

# --------------------------
# 8️⃣ Квартильний аналіз викладачів
# --------------------------

clean_stats_22_23 = stats_22_23  
clean_stats_23_24 = stats_23_24

# Cтворюємо копію датафрейму без викладачів, які потрапили у викиди
clean_stats_22_23 = stats_22_23[~stats_22_23["name_normalized"].isin(outliers_teachers_all)].copy()
clean_stats_23_24 = stats_23_24[~stats_23_24["name_normalized"].isin(outliers_teachers_all)].copy()

quartile_lists_22_23 = analytics.get_quartile_category_teachers_list(clean_stats_22_23)
quartile_lists_23_24 = analytics.get_quartile_category_teachers_list(clean_stats_23_24)


# Об’єднуємо списки за два роки
best_teachers_all = quartile_lists_22_23["best"] + quartile_lists_23_24["best"]
interquart_teachers_all = quartile_lists_22_23["interquartile"] + quartile_lists_23_24["interquartile"]
bad_teachers_all = quartile_lists_22_23["bad"] + quartile_lists_23_24["bad"]
worst_teacher_all = outliers_teachers_all

# Категоризація викладачів по признаку якості ("best", "interquart", "bad", "worst") і 
# стабільності роботи в школі.

def get_teacher_analysis( best_all,
    interquart_all,
    bad_all, 
    outliers_all     
):
    '''
    Функція створює датафрейм з серіями по горизонталі - прізвищами і іменами викладачів,
    серії по вертикалі є категорії викладачів:
        - "best"
        - "interquart"
        - "bad"
        - "worst"
    значення наступні:
    0 - викладач не належить до цієї категорії
    1 - викладач належить до цієї категорії     

    Аргументи:
    best_all, interquart_all, bad_all, outliers_all - листи з прізвищами і іменами викладачів
    за два навчальних роки 2022/2023 і 2023/2024

    Повертає: Dataframe     

    '''
    # Рахуємо кількість появ у кожній категорії
    best_counts = Counter(best_all)
    interquart_counts = Counter(interquart_all)
    bad_counts = Counter(bad_all)
    outliers_counts = Counter(outliers_all)

    # Всі унікальні імена викладачів
    all_names = set(best_all) | set(interquart_all) | set(bad_all) | set(outliers_all)

    # Створюємо датафрейм
    df = pd.DataFrame({'name': list(all_names)})

    # Додаємо стовпці з 0, 1, 2.. (в залежності скількі разів зустрічається викладач в кожній категорії)
    df['best'] = df['name'].apply(lambda x: best_counts.get(x, 0))
    df['interquart'] = df['name'].apply(lambda x: interquart_counts.get(x, 0))
    df['bad'] = df['name'].apply(lambda x: bad_counts.get(x, 0))
    df['worst'] = df['name'].apply(lambda x: outliers_counts.get(x, 0))

    return df

df_analysis = get_teacher_analysis(best_teachers_all,
    interquart_teachers_all,
    bad_teachers_all, 
    outliers_teachers_all)
df_analysis.to_csv("data/teachers_analysis.csv", index=False)


