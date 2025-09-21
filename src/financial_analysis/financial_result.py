"""
Назва файлу: financial_result.py
Мета: Оцінити фінансовий ефект від впровадження системи бонусації викладачів з назвою 
"Bonus_v1", яка впливає на зниження відтоку клієнтів в 2,47 разів (OR, модель Logit),
та представити результати у вигляді графіків.
    
Вхідні дані:
    - "data/Monthly_financial_report_2023_2024.csv" - датасет з сайту аналітики компанії, який містить 
    фінансові показники про запланований прибуток і втрачений в результаті відтоку клієнтів;
    - "doc/Monthly_financial_report_2023_2024.md" - опис вхідного датасету з фінансовими результатами;
    - "data/results_of_modeling/Bonus_v1_v2.csv" - датасет з розрахунком бонусів по запропонованим 
    системам Bonus_v1 i Bonus_v2 і загального доходу викладачів;

Вихідні результати:
    - "data/financial_results/final_financial_results.csv" - розрахунок прибутку компанії за рахунок 
    введення системи бонусації "Bonus_v1";
    - Висновки у файлі financial_result.md

Вихідні графіки:
    - "images/financial_plots/scatter_growth.png" (діаграма розсіювання значень зростання прибутку 
    відносно зростання видатків);
    - "images/financial_plots/bar_growth.png" (стовпчаста діаграма, яка показує коефіцієнт зростання
    видатків і прибутку).
    
Автор: Olga Glubokaya
"""
import os
import logging
import pandas as pd
import matplotlib.pyplot as plt

# -------------------- Конфігурація --------------------
OR = 2.47          # Коефіціент зниження відтоку клієнтів
COURSE_MONTH = 9   # Тривалість одного курсу в місяцях

# Шляхи до файлів
FINANCIAL_FILE = "data/Monthly_financial_report_2023_2024.csv"
BONUS_FILE = "data/results_of_modeling/Bonus_v1_v2.csv"
OUTPUT_CSV = "data/financial_results/final_financial_results.csv"
SCATTER_IMG = "images/financial_plots/scatter_growth.png"
BAR_IMG = "images/financial_plots/bar_growth.png"

# Налаштування логування
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


# -------------------- Функції --------------------
def read_data(financial_file: str, bonus_file: str):
    """
    Зчитує та попередньо обробляє фінансові й бонусні дані для подальшого аналізу.

    Параметри
    ---------
    financial_file : str
        Шлях до CSV-файлу з фінансовими показниками (місячний дохід, витрати, прибуток тощо).
    bonus_file : str
        Шлях до CSV-файлу з даними про бонуси викладачів.

    Дії
    ----
    1. Зчитує обидва файли у DataFrame.
    2. Перетворює стовпець "Month" у формат `YYYY-MM` для узгодженості між датасетами.
    3. Логує повідомлення про успішне зчитування та обробку даних.

    Повертає
    --------
    tuple[pd.DataFrame, pd.DataFrame]
        - `df_financial`: DataFrame з фінансовими даними.
        - `df_bonus`: DataFrame з даними про бонуси викладачів.

    Логування
    ---------
    Створює запис рівня INFO: "Дані успішно зчитані та оброблені."
    """

    df_financial = pd.read_csv(financial_file)
    df_bonus = pd.read_csv(bonus_file)

    # Перетворення Month у datetime у формат YYYY-MM
    df_financial["Month"] = pd.to_datetime(df_financial["Month"]).dt.strftime("%Y-%m")
    df_bonus["Month"] = pd.to_datetime(df_bonus["Month"]).dt.strftime("%Y-%m")

    logging.info("Дані успішно зчитані та оброблені.")
    return df_financial, df_bonus


def calculate_bonus_growth(df_financial: pd.DataFrame, df_bonus: pd.DataFrame):
    """
    Розраховує ключові коефіцієнти зростання бонусів та їхній вплив на фінансові показники.

    Параметри
    ---------
    df_financial : pd.DataFrame
        Фінансові дані з показниками витрат, прибутку, кількості клієнтів тощо.
    df_bonus : pd.DataFrame
        Дані про бонуси викладачів, включно з їх загальною компенсацією та доходом.

    Операції
    --------
    1. Обчислює коефіцієнт зростання бонусів (`Bonus_growth_coefficient`).
    2. Розраховує середнє та стандартне відхилення цього коефіцієнта для кожного місяця.
    3. Об’єднує результати з фінансовими даними.
    4. Логує попередження, якщо стандартне відхилення перевищує 17% від середнього.
    5. Розраховує:
       - `Expenses_Bonus_v1` – витрати з урахуванням бонусації.
       - `Lost_clients_Bonus_v1` – скориговану кількість втрачених клієнтів з урахуванням Odds Ratio.
       - `Profit_Bonus_v1` – прибуток із застосуванням нової системи бонусів.
       - `Grow_expenses` – коефіцієнт зростання витрат.
       - `Grow_profit` – коефіцієнт зростання прибутку.
    6. Обчислює середньорічні значення `Grow_expenses` та `Grow_profit` і виводить їх у лог.

    Повертає
    --------
    pd.DataFrame
        Фінансовий DataFrame із доданими розрахованими колонками.

    Логування
    ---------
    - INFO: Успішний розрахунок та середньорічні коефіцієнти.
    - WARNING: Якщо стандартне відхилення Bonus Growth Coefficient > 17%.
    """

    # 1. Bonus_growth_coefficient
    df_bonus["Bonus_growth_coefficient"] = df_bonus["Total_income_v1"] / df_bonus["Total_compensation"]

    # Середньомісячне значення та std
    monthly_avg = df_bonus.groupby("Month")["Bonus_growth_coefficient"].mean().round(2).reset_index()
    monthly_avg.rename(columns={"Bonus_growth_coefficient": "Avg_bonus_growth_coef"}, inplace=True)
    monthly_std = df_bonus.groupby("Month")["Bonus_growth_coefficient"].std().round(2).reset_index()
    monthly_std.rename(columns={"Bonus_growth_coefficient": "Std_bonus_growth_coef"}, inplace=True)

    # Об'єднуємо з фінансовим df
    df = df_financial.merge(monthly_avg, on="Month", how="left")
    df = df.merge(monthly_std, on="Month", how="left")

    # Перевірка Std < 17% від Avg
    percent_std = df["Std_bonus_growth_coef"] * 100 / df["Avg_bonus_growth_coef"]
    if (percent_std < 17).all():
        logging.info("Стандартне відхилення BGC в межах норми (<17%).")
    else:
        logging.warning("Стандартне відхилення BGC перевищує 17%!")

    # 2. Expenses_Bonus_v1
    df["Expenses_Bonus_v1"] = round(df["Actual_expenses"] * df["Avg_bonus_growth_coef"], 2)

    # 3. Lost_clients_Bonus_v1
    df["Lost_clients_Bonus_v1"] = round(df["Lost_clients"] / OR, 2)

    # 4. Profit_Bonus_v1
    df["Profit_Bonus_v1"] = round((
        df["Course cost"] * (df["Planned_clients"] - df["Lost_clients_Bonus_v1"]) * COURSE_MONTH
        - df["Expenses_Bonus_v1"]
    ), 2)

    # 5. Grow_expenses
    df["Grow_expenses"] = round(df["Expenses_Bonus_v1"] / df["Actual_expenses"], 2)

    # 6. Grow_profit
    df["Grow_profit"] = round(df["Profit_Bonus_v1"] / df["Actual_profit"], 2)

    # Середньорічні коефіцієнти
    grow_expenses_mean = df["Grow_expenses"].mean()
    grow_profit_mean = df["Grow_profit"].mean()
    logging.info(f"Середньорічний коефіцієнт зростання витрат: {grow_expenses_mean:.2f}")
    logging.info(f"Середньорічний коефіцієнт зростання прибутку: {grow_profit_mean:.2f}")

    return df


def save_results(df: pd.DataFrame, output_csv: str):
    """
    Зберігає фінансові результати у CSV-файл.

    Параметри
    ---------
    df : pd.DataFrame
        DataFrame із фінансовими показниками та розрахованими коефіцієнтами,
        які потрібно експортувати.
    output_csv : str
        Шлях до файлу для збереження результатів (включно з ім’ям файлу).

    Операції
    --------
    1. Створює директорію для вихідного файлу, якщо вона ще не існує.
    2. Зберігає DataFrame у форматі CSV без індексів.
    3. Логує повідомлення про успішне збереження.

    Логування
    ---------
    - INFO: підтвердження шляху, куди збережено результати.
    """

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    df.to_csv(output_csv, index=False)
    logging.info(f"Фінансові результати збережено у {output_csv}")


def plot_scatter(df: pd.DataFrame, scatter_img: str):
    """
    Будує та зберігає scatter plot для порівняння зростання прибутку і витрат.

    Параметри
    ---------
    df : pd.DataFrame
        DataFrame з розрахованими коефіцієнтами зростання витрат (`Grow_expenses`)
        та прибутку (`Grow_profit`).
    scatter_img : str
        Шлях до файлу зображення (включно з ім’ям файлу), куди буде збережено графік.

    Операції
    --------
    1. Створює директорію для збереження, якщо вона не існує.
    2. Будує scatter plot:
       - вісь X — коефіцієнт зростання витрат;
       - вісь Y — коефіцієнт зростання прибутку;
       - додає лінію y=x для візуальної оцінки ефективності.
    3. Налаштовує заголовок, підписи осей, легенду та сітку.
    4. Зберігає графік у форматі PNG із високою роздільною здатністю.
    5. Відображає графік на екрані.
    6. Логує повідомлення про місце збереження зображення.

    Візуальний сенс
    ---------------
    - Точки вище лінії y=x означають, що прибуток зростав швидше за витрати.
    - Точки нижче лінії y=x — витрати зростали швидше, ніж прибуток.
    """

    os.makedirs(os.path.dirname(scatter_img), exist_ok=True)

    x = df["Grow_expenses"]
    y = df["Grow_profit"]

    plt.figure(figsize=(8, 6))
    plt.scatter(x, y, color="blue", alpha=0.6, label="Дані")
    max_val = max(x.max(), y.max())
    plt.plot([0, max_val], [0, max_val], color="red", linestyle="--", label="y = x")
    plt.xlabel("Зростання видатків (рази)")
    plt.ylabel("Зростання прибутку (рази)")
    plt.title("Зростання прибутку відносно зростання видатків")
    plt.legend()
    plt.grid(True)
    plt.savefig(scatter_img, dpi=300, bbox_inches="tight")
    plt.show()
    logging.info(f"Scatter plot збережено у {scatter_img}")


def plot_bar(df: pd.DataFrame, bar_img: str):
    """
    Будує та зберігає bar chart середніх коефіцієнтів зростання витрат і прибутку.

    Параметри
    ---------
    df : pd.DataFrame
        DataFrame з колонками `Grow_expenses` і `Grow_profit`, що містять 
        коефіцієнти зростання для кожного місяця.
    bar_img : str
        Шлях до файлу зображення (включно з ім’ям файлу), куди буде збережено bar chart.

    Операції
    --------
    1. Створює директорію для збереження, якщо вона не існує.
    2. Обчислює середнє значення зростання витрат та прибутку.
    3. Будує bar chart із двома стовпчиками:
       - перший показує середнє зростання витрат,
       - другий — середнє зростання прибутку.
    4. Додає підписи зі значеннями над стовпчиками для зручності читання.
    5. Налаштовує підписи осей і заголовок графіка.
    6. Зберігає графік у форматі PNG із високою роздільною здатністю.
    7. Відображає графік на екрані.
    8. Логує повідомлення про місце збереження зображення.

    Візуальний сенс
    ---------------
    Графік дозволяє швидко порівняти середні темпи зростання витрат та прибутку.
    Якщо стовпчик прибутку вищий за стовпчик витрат — система бонусації 
    працює ефективно.
    """

    os.makedirs(os.path.dirname(bar_img), exist_ok=True)

    avg_expenses = df["Grow_expenses"].mean()
    avg_profit = df["Grow_profit"].mean()

    plt.figure(figsize=(6, 5))
    bars = plt.bar(["Зростання видатків", "Зростання прибутку"],
                   [avg_expenses, avg_profit],
                   color=["#9C57D7", "#00BFC4"])
    plt.ylabel("Кратність зростання")
    plt.title("Середнє зростання витрат і прибутку")

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height, f'{height:.2f}', 
                 ha='center', va='bottom', fontsize=10)

    plt.savefig(bar_img, dpi=300, bbox_inches="tight")
    plt.show()
    logging.info(f"Bar chart збережено у {bar_img}")


# -------------------- Main --------------------
def main():
    df_financial, df_bonus = read_data(FINANCIAL_FILE, BONUS_FILE)
    df_final = calculate_bonus_growth(df_financial, df_bonus)
    save_results(df_final, OUTPUT_CSV)
    plot_scatter(df_final, SCATTER_IMG)
    plot_bar(df_final, BAR_IMG)


if __name__ == "__main__":
    main()
