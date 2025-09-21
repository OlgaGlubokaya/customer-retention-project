"""
Модуль: financial_losses.py

Мета:
------
Аналітика фінансових втрат викладачів і школи, розрахунок максимально можливого доходу,
відсотку втрат і візуалізація результатів у вигляді кругових діаграм.

Функціонал:
-----------
1. Завантаження та фільтрація даних по датах.
2. Обчислення середніх та стандартних відхилень втрат.
3. Підрахунок кількості груп для кожного викладача.
4. Розрахунок максимально можливого доходу та втрат викладачів і школи.
5. Збереження ключових показників у CSV.
6. Візуалізація втрат у вигляді кругових діаграм.

Вхідні дані:
------------
- "data/General_Gone_Clients_Report.csv" — основний файл з інформацією про групи, зарплати та відвали.
- Періоди для аналізу (start_date, end_date).

Вихідні дані:
-------------
- CSV-файли з ключовими фінансовими показниками для кожного періоду:

    - output_csv="financial_results/financial_analysis_2022_2023.csv"
    - output_csv="financial_results/financial_analysis_2023_2024.csv"

- PNG-файли з круговими діаграмами втрат:
    - output_chart="images/financial_plots/loss_pie_chart_2022_2023.png",
    - output_chart="images/financial_plots/loss_pie_chart_2023_2024.png"
"""

import pandas as pd
import matplotlib.pyplot as plt

# Постійні параметри
NUMBER_LESSONS = 40
MONTHS_IN_YEAR = 12


def load_and_filter_data(file_path, start_date, end_date):
    '''
    Функція фільтрує дані в датафреймі по стартової і кінцевої датах.
    file_path - назва csv-файлу ("General_Gone_Clients_Report.csv")
    start_date, end_date - відповідно початкова і кінцева дати

    @return - відсортований датафрейм
    '''
    df = pd.read_csv(file_path)
    df["start_date"] = pd.to_datetime(df["start_date"], errors='coerce')
    return df[(df["start_date"] >= pd.Timestamp(start_date)) & (df["start_date"] <= pd.Timestamp(end_date))].copy()


def calculate_loss_stats(df):
    '''
    Функція рахує середне статистичне і стандартне відхилення показників втрат викладачів і компанії.
    Показники "teacher_lost" і "school_lost" взято з бази даних компанії, які згуповано і підсумовано в 
    файлі "costs_salaries_converted.csv"

    Показники "teacher_lost" в кожної серії розраховані по формулі:
    кількість невідвіданих уроків * на вартість заняття

    Показники "school_lost":
    вартість одного уроку * кількість невідвіданих занять - видатки на кожну групу

    df - датафрейм з даними для аналізу "costs_salaries_converted.csv"

    @return - середне і стандартне відхилення по втратам викладача і компанії.
    '''
    mean_lost = df["teacher_lost"].mean()
    std_lost = df["teacher_lost"].std()
    mean_lost_comp = df["school_lost"].mean()
    std_lost_comp = df["school_lost"].std()
    return mean_lost, std_lost, mean_lost_comp, std_lost_comp


def calculate_teacher_groups(df):
    '''
    Рахує кількість груп у викладача
    df - датафрейм "General_Gone_Clients_Report.csv"
    @return - датафрейм з прізвищами і іменами викладачів і загальною кількістю їх груп

    '''
    unique_groups = df.drop_duplicates(subset=["name_normalized", "group_id"])
    group_counts_df = unique_groups["name_normalized"].value_counts().reset_index()
    group_counts_df.columns = ["name_normalized", "group_count"]
    return group_counts_df


def calculate_teacher_loss(group_counts_df, mean_lost, avg_salary):
    '''
    Функція розраховує максимально можливий дохід викладача в школі і максимально можливі втрати
    group_counts_df - датафрейм з даними про кількість груп по кожному викладачу
    mean_lost - середне значення втрат викладача від відвалів
    avg_salary - середне значення доходу викладачів

    @return - max_profit, max_loss - максимальний дохід і максимальні втрати викладача
    '''
    max_groups = group_counts_df["group_count"].max()

    # Формула розрахунку річного доходу викладача:
    # Кіль-ть груп * Кіль-ть уроків * Середню оплату за один урок * Кіль-ть місяців в році 
    max_profit = max_groups * NUMBER_LESSONS * avg_salary * MONTHS_IN_YEAR

    # Формула розрахунку річних втрат викладача:
    # Середньорічні втрати на одну групу * Кіль-ть груп
    max_loss = mean_lost * max_groups
    return max_profit, max_loss


def calculate_company_loss(df, mean_lost_comp):
    """
    Функція розраховує максимально можливий дохід компанії і максимально можливі втрати
    df - датафрейм з даними, витягнутими з бази даних (файл "General_Gone_Clients_Report.csv")
    mean_lost_comp - середне значення втрат школи від відвалів
    
    @return - group_number - кількість груп без повторення
            - mean_lesson_cost - середня вартість курсу
            - profit_school_year - річний дохід школи
            - loss_school_year - річні втрати школи
    """
    group_number = df.drop_duplicates(subset=["group_id"])["group_id"].count()
    mean_lesson_cost = df["course_cost_in_month"].mean()

    # Формула розрахунку середньорічного доходу (Revenue) компанії:
    # Середній дохід з однієї групи * кількість груп в рік * кількість місяців у році
    profit_school_year = mean_lesson_cost * group_number * MONTHS_IN_YEAR

    # Формула розрахунку річних втрат компанії:
    # Середньорічні втрати на одну групу * Кіль-ть груп
    loss_school_year = mean_lost_comp * group_number
    return group_number, mean_lesson_cost, profit_school_year, loss_school_year


def plot_loss_pies(teacher_loss_percent, company_loss_percent, output_file):
    """
    Функція малює графік - кругову діаграму по втратам школи і викладача окремо.
    teacher_loss_percent - відсоток втрат викладачів
    company_loss_percent - відсоток втрат школи
    output_file - назва файлу для збереження графіка
    
    """
    teacher_data = [teacher_loss_percent, 100 - teacher_loss_percent]
    company_data = [company_loss_percent, 100 - company_loss_percent]

    fig, axs = plt.subplots(1, 2, figsize=(12, 6))
    axs[0].pie(teacher_data, labels=['Втрати викладача (%)', 'Збережено (%)'],
               autopct='%1.1f%%', colors=['#FF9999', '#99FF99'], startangle=90)
    axs[0].axis('equal')
    axs[0].set_title('Середньорічні втрати викладача')

    axs[1].pie(company_data, labels=['Втрати школи (%)', 'Збережено (%)'],
               autopct='%1.1f%%', colors=['#FFCC99', '#99CCFF'], startangle=90)
    axs[1].axis('equal')
    axs[1].set_title('Середньорічні втрати школи')

    plt.tight_layout()
    plt.savefig(output_file, dpi=300)
    plt.show()


def analyze_losses(file_path, start_date, end_date, output_chart, output_csv):
    """
    Функція проводить аналіз з застосуванням попередніх функцій.
    file_path - основний файл з даними по відвалам компанії "General_Gone_Clients_Report.csv"
    start_date - дата початку періода
    end_date - дата кінця розрахункового періоду
    output_chart - назва графіку

    @return - файл .csv зі збереженими фінансовими даними.

    """
    df_filtered = load_and_filter_data(file_path, start_date, end_date)
    mean_lost, std_lost, mean_lost_comp, std_lost_comp = calculate_loss_stats(df_filtered)

    group_counts_df = calculate_teacher_groups(df_filtered)
    avg_salary = df_filtered["teacher_salaries"].mean()

    max_teacher_profit, max_loss_teacher = calculate_teacher_loss(group_counts_df, mean_lost, avg_salary)
    teacher_loss_percent = round(max_loss_teacher * 100 / max_teacher_profit, 2)

    group_number, mean_lesson_cost, profit_school_year, loss_school_year = calculate_company_loss(df_filtered, mean_lost_comp)
    company_loss_percent = round(loss_school_year * 100 / profit_school_year, 2)

    # Збереження результатів у CSV
    results_df = pd.DataFrame([{
        "Period": f"{start_date} — {end_date}",
        "Teacher_Mean_Loss": round(mean_lost, 2),
        "Teacher_Std_Loss": round(std_lost, 2),
        "Company_Mean_Loss": round(mean_lost_comp, 2),
        "Company_Std_Loss": round(std_lost_comp, 2),
        "Avg_Salary": round(avg_salary, 2),
        "Max_Teacher_Profit": round(max_teacher_profit, 2),
        "Teacher_Loss_%": teacher_loss_percent,
        "Group_Number": group_number,
        "Mean_Lesson_Cost": round(mean_lesson_cost, 2),
        "Profit_School_Year": round(profit_school_year, 2),
        "Company_Loss_%": company_loss_percent
    }])

    results_df.to_csv(output_csv, index=False)

    # Побудова графіку
    plot_loss_pies(teacher_loss_percent, company_loss_percent, output_chart)


# Виклики для різних періодів
analyze_losses(
    file_path="data/General_Gone_Clients_Report.csv",
    start_date="2022-08-01",
    end_date="2023-07-31",
    output_chart="images/financial_plots/loss_pie_chart_2022_2023.png",
    output_csv="data/financial_results/financial_analysis_2022_2023.csv"
)

analyze_losses(
    file_path="data/General_Gone_Clients_Report.csv",
    start_date="2023-08-01",
    end_date="2024-07-31",
    output_chart="images/financial_plots/loss_pie_chart_2023_2024.png",
    output_csv="data/financial_results/financial_analysis_2023_2024.csv"
)
