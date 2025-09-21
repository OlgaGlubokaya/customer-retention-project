"""
Назва файлу: time_series_analysis.py

Мета:
-----
Аналіз часових рядів викладачів за ключовими освітніми і бізнес показниками ефективності та 
порівняння різних категорій викладачів між собою.

Функціонал:
-----------
1. Завантаження даних про викладачів та їхні показники (CSV-файли).
2. Вибір викладачів з різних категорій (Stable vs Unstable (A vs B), Best i Bad (C vs D)).
3. Розрахунок описової статистики:
   - середні значення метрик(mean)
   - медіани метрик (median)
4. Статистичне порівняння категорій за допомогою тесту Mann–Whitney U.
5. Візуалізація розподілу метрик у вигляді boxplot-графіків.
6. Збереження результатів у вигляді:
   - DataFrame зі статистикою -> csv файл
   - DataFrame з результатами Mann–Whitney -> csv файл
   - графіків у форматі .png

Метрики для аналізу:
--------------------
- Feedback_for_parents       : % написаних фідбеків у чат батьків
- Feedback_to_students       : % написаних фідбеків у чат студентів
- Control_of_homework        : % перевірених домашніх завдань
- Control_of_potential_loss  : % повідомлень тьютору про ризик відвалу
- Average_success            : середня успішність учнів (%)

Категорії викладачів:
---------------------
- A (Stable)        : два роки поспіль у тій самій категорії (best / interquart)
- B (Unstable)      : два роки у різних категоріях

- C (Best_One_Year) : один рік, серед кращих (best)
- D (Bad_One_Year)  : один рік, серед гірших (bad)

Структура коду:
---------------
- calculate_summary()    : обчислення середніх та медіан
- run_mannwhitney()      : тест Mann–Whitney U для всіх метрик
- plot_metrics()         : побудова boxplot-графіків
- compare_teacher_categories():
    головна функція, що об'єднує попередні та повертає результати у вигляді словника

Вхідні дані:
---------------------
"data/teachers_A.csv" - ранжовані викладачі за ознакою "Стабільні"
"data/teachers_B.csv" - ранжовані викладачі за ознакою "Нестабільні"
"data/teachers_C.csv" - ранжовані викладачі за ознакою "Best_One_Year"
"data/teachers_D.csv" - ранжовані викладачі за ознакою "Bad_One_Year"
"data/Analysis_teacher_with_importance.csv" - дані стосовно KPI викладачів, їх відвалів і 
важливості кожного KPI у відвалах по кожному викладачу.

Вихідні дані:
---------------------
"data/results_mannwhitney/results_AB_mean.csv"
"data/results_mannwhitney/results_AB_median.csv"
"data/results_mannwhitney/results_AB_mannwhitney.csv"

"data/results_mannwhitney/results_CD_mean.csv"
"data/results_mannwhitney/results_CD_median.csv"
"data/results_mannwhitney/results_CD_mannwhitney.csv"

"image/AB_analysis_Stable_vs_Unstable.png" - графік boxplot - порівняння квартильних KPI AB-ранків викладачів
"image/CD_analysis_Best_One_Year_vs_Bad_One_Year.png" - графік boxplot - порівняння квартильних KPI CD-ранків викладачів

"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import mannwhitneyu
from pathlib import Path


def calculate_summary(df_all, metrics):
    """
    Обчислює описову статистику (середнє та медіану) для заданих метрик у розрізі категорій.

    Parameters
    ----------
    df_all : pandas.DataFrame
        Вхідний датафрейм, що містить стовпець "category" (групувальна змінна) 
        та числові метрики для аналізу.
    metrics : list of str
        Список назв колонок (метрик), для яких потрібно розрахувати статистику.

    Returns
    -------
    summary_mean : pandas.DataFrame
        Таблиця з середніми значеннями по кожній категорії для вибраних метрик.
        Колонки: ["category"] + metrics.
    summary_median : pandas.DataFrame
        Таблиця з медіанними значеннями по кожній категорії для вибраних метрик.
        Колонки: ["category"] + metrics.
    """
    summary_mean = df_all.groupby("category")[metrics].mean().reset_index().round(2)
    summary_median = df_all.groupby("category")[metrics].median().reset_index().round(2)
    return summary_mean, summary_median

def run_mannwhitney(df1, df2, metrics):
    """
    Виконує Mann–Whitney U тест для порівняння розподілів значень по кожній метриці 
    між двома групами.

    Parameters
    ----------
    df1 : pandas.DataFrame
        Датафрейм викладачів першої порівняльної категорії, що містить числові метрики для порівняння.
    df2 : pandas.DataFrame
        Датафрейм викладачів другої порівняльної категорії, що містить ті ж числові метрики.
    metrics : list of str
        Список назв колонок (метрик), для яких потрібно виконати Mann–Whitney тест по двом визначаємим
        категоріям.

    Returns
    -------
    pandas.DataFrame
        Датафрейм з результатами тесту для кожної метрики, з колонками:
        - "metric" — назва метрики,
        - "U_stat" — значення статистики U тесту (округлене до 2 знаків),
        - "p_value" — точне p-value (округлене до 4 знаків),
        - "significant" — логічне значення True, якщо p < 0.05, інакше False.
    """
    results = []
    for metric in metrics:
        vals1 = df1[metric].dropna()
        vals2 = df2[metric].dropna()
        if len(vals1) > 0 and len(vals2) > 0:
            stat, p = mannwhitneyu(vals1, vals2, alternative='two-sided')
            results.append({
                "metric": metric,
                "U_stat": round(stat, 2),
                "p_value": round(p, 4),
                "significant": p < 0.05
            })
    return pd.DataFrame(results)


def plot_metrics(df_all, metrics, group1_name, group2_name, output_prefix):
    """
    Побудова та збереження boxplot графіків для порівняння розподілу метрик між двома групами.

    Parameters
    ----------
    df_all : pandas.DataFrame
        Датафрейм, що містить всі дані обох категорій викладачів, включно з колонкою "category".
    metrics : list of str
        Список назв числових колонок (метрик), для яких будуть побудовані графіки.
    group1_name : str
        Назва першої категорії викладачів (для підписів та імені файлу).
    group2_name : str
        Назва другої категорії викладачів (для підписів та імені файлу).
    output_prefix : str
        Префікс для імені файлу графіка, до якого додадуться назви груп.

    Returns
    -------
    pathlib.Path
        Шлях до збереженого PNG-файлу з boxplot графіками.
    """
    sns.set_theme(style="whitegrid")
    n = len(metrics)
    rows = (n + 2) // 3
    plt.figure(figsize=(5 * 3, 4 * rows))

    for i, metric in enumerate(metrics, 1):
        plt.subplot(rows, 3, i)
        sns.boxplot(data=df_all, x="category", y=metric, hue="category", palette="Set2", legend=False)
        plt.title(metric)
        plt.xlabel("")
        plt.ylabel(metric)

    plt.tight_layout()
    output_file = Path(f"{output_prefix}_{group1_name}_vs_{group2_name}.png")
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()
    return output_file


def compare_teacher_categories(file_group1, file_group2, file_rates,
                               group1_name="Group1", group2_name="Group2",
                               metrics=None, output_prefix="Comparison"):
    """
    Комплексне порівняння двох категорій викладачів за набором метрик.

    Функціонал
    ----------
    1. Завантажує дані по групах викладачів та їхнім оцінкам з CSV.
    2. Фільтрує потрібних викладачів для кожної групи.
    3. Додає колонку "category" для позначення групи.
    4. Розраховує середні та медіани по кожній метриці.
    5. Виконує Mann–Whitney U тест для порівняння розподілів метрик між групами.
    6. Побудова та збереження boxplot графіків для всіх метрик.

    Parameters
    ----------
    file_group1 : str
        Шлях до CSV-файлу першої викладачів першої порівняльної категорії.
    file_group2 : str
        Шлях до CSV-файлу викладачів другої групи порівняльної категорії.
    file_rates : str
        Шлях до CSV-файлу з даними метрик викладачів.
    group1_name : str, optional
        Назва першої категорії для підписів та імені файлу (default="Group1").
    group2_name : str, optional
        Назва другої категорії для підписів та імені файлу (default="Group2").
    metrics : list of str, optional
        Список назв метрик для аналізу. Якщо None, використовується стандартний набір.
    output_prefix : str, optional
        Префікс для імені файлу з графіками (default="Comparison").

    Returns
    -------
    dict
        Словник з результатами:
        - "mean": DataFrame із середніми значеннями метрик по групах.
        - "median": DataFrame із медіанами по групах.
        - "mannwhitney": DataFrame з результатами Mann–Whitney U тесту по кожній метриці.
        - "plot_file": pathlib.Path до збереженого PNG-файлу з графіками.

    Notes
    -----
    - Функція виконує комплексну аналітику для порівняння ефективності викладачів.
    - Boxplot графіки зберігаються у високій роздільній здатності (300 dpi).
    """

    if metrics is None:
        metrics = [
            "Feedback_for_parents",
            "Feedback_to_students",
            "Control_of_homework",
            "Control_of_potential_loss",
            "Average_success"
        ]

    # Завантаження даних
    group1_df = pd.read_csv(file_group1) # викладачи категорії номер один в порівнянні
    group2_df = pd.read_csv(file_group2) # викладачи категорії номер два в порівнянні
    df_rates = pd.read_csv(file_rates) # датафрейм з метриками кожного викладача

    # Вибір потрібних викладачів по двом порівняльним категоріям
    names1 = group1_df["name"].unique()
    names2 = group2_df["name"].unique()

    df1 = df_rates[df_rates["teacher_name"].isin(names1)].copy()
    df2 = df_rates[df_rates["teacher_name"].isin(names2)].copy()

    df1["category"] = group1_name # додаємо колонку для першої категорії
    df2["category"] = group2_name # додаємо колонку для другої категорії

    df_all = pd.concat([df1, df2], ignore_index=True)

    # 1. Статистика по усім KPI викладачів
    summary_mean, summary_median = calculate_summary(df_all, metrics)

    # 2. Mann–Whitney по ранкам викладачів за обраною ознакою
    mannwhitney_results = run_mannwhitney(df1, df2, metrics)

    # 3. Візуалізація
    plot_file = plot_metrics(df_all, metrics, group1_name, group2_name, output_prefix)

    return {
        "mean": summary_mean,
        "median": summary_median,
        "mannwhitney": mannwhitney_results,
        "plot_file": plot_file
    }


# 🔹 Реалізація порівняння виконання мертик двома категоріями викладачів: "Stable" vs "Unstable"
results_AB = compare_teacher_categories(
    file_group1="data/teachers_A.csv",
    file_group2="data/teachers_B.csv",
    file_rates="data/Analysis_teachers_rate.csv",
    group1_name="Stable",
    group2_name="Unstable",
    output_prefix="images/AB_analysis"
)

# 🔹 Реалізація порівняння виконання мертик двома категоріями викладачів: "Best_One_Year" vs "Bad_One_Year"
results_CD = compare_teacher_categories(
    file_group1="data/teachers_C.csv",
    file_group2="data/teachers_D.csv",
    file_rates="data/Analysis_teachers_rate.csv",
    group1_name="Best_One_Year",
    group2_name="Bad_One_Year",
    output_prefix="images/CD_analysis"
)

# Збереження результатів
results_AB["mean"].to_csv("data/results_mannwhitney/results_AB_mean.csv", index=False)
results_AB["median"].to_csv("data/results_mannwhitney/results_AB_median.csv", index=False)
results_AB["mannwhitney"].to_csv("data/results_mannwhitney/results_AB_mannwhitney.csv", index=False)

print("Графік збережено тут:", results_AB["plot_file"])

results_CD["mean"].to_csv("data/results_mannwhitney/results_CD_mean.csv", index=False)
results_CD["median"].to_csv("data/results_mannwhitney/results_CD_median.csv", index=False)
results_CD["mannwhitney"].to_csv("data/results_mannwhitney/results_CD_mannwhitney.csv", index=False)

print("Графік збережено тут:", results_CD["plot_file"])


