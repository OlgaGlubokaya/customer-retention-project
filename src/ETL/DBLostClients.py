"""
Назва файлу: DBLostClients.py
Мета:
ETL-модуль для роботи з даними клієнтів, які припинили навчання у школі 
за період з січня 2022 по липень 2024 року.

Призначення модуля
------------------
1. Побудова локальної SQLite-бази (`Gone_Clients.sqlite`) з нормалізованими таблицями:
   - students (студенти)
   - teachers (викладачі)
   - groups (групи)
   - gone_clients_content (факти відходу студентів із зазначенням причин та відвідуваності)

2. Завантаження та очищення даних:
   - підготовка списку студентів (ім’я, ID, вік)
   - формування таблиці викладачів (ім’я, предмет, місто)
   - формування таблиці груп (назва, ID, дата початку)
   - об’єднання відвідуваності з інформацією про викладачів
   - заповнення пропущених викладачів вручну для окремих груп

3. Завантаження інтегрованих даних у таблицю `gone_clients_content`.

Основні функції
---------------
- open(), close(), do() — службові методи для роботи з БД
- create() — створення структури таблиць у SQLite
- get_students(), get_teachers(), get_groups() — заповнення довідникових таблиць
- unite_tables_csv() — об’єднання відвідуваності з викладачами, збереження у CSV
- insert_into_gone_clients_content() — завантаження фінальних даних у БД
- main() — керуюча функція, яка запускає етапи ETL-процесу

Примітка
--------
Модуль розроблено для демонстрації навичок ETL, Data Cleaning та роботи з базами даних
у межах side-project з аналізу клієнтського відтоку.

Джерела даних
-------------
Вхідні дані:
   - data/1_Attended_classes.csv - таблиця містить опис студентів, що покинули навчання (відвалів), включно
    з причиною, яку озвучили батьки, і віком дитини;
   - data/Extended_raw_data.csv - таблиця з аналітичного додатку школи, яка містить додатковий опис
    відвалів, а саме дані про предмет, який вивчався, місто і викладача школи.

Вихідні результати:
    - data/Gone_Clients.sqlite - база даних клієнтів, які припинили навчання в школі.
"""

import pandas as pd
import sqlite3

db_name = 'data/Gone_Clients.sqlite'
conn = None
cursor = None
df1 = pd.read_csv("data/1_Attended_classes.csv")
df2 = pd.read_csv("data/Extended_raw_data.csv")

def open():
    """
    Відкриває з'єднання з базою даних SQLite.
    Робить доступними глобальні змінні conn і cursor.
    Вмикає підтримку зовнішніх ключів.
    """
    global conn, cursor
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")


def close():
    """
    Закриває курсор та з'єднання з базою даних.
    Викликати після завершення роботи.
    """
    cursor.close()
    conn.close()

def do(query):
    """
    Виконує SQL-запит (SELECT, INSERT, UPDATE, DELETE тощо).
    """
    cursor.execute(query)
    conn.commit()
    
def create():
    """
    Створює структуру бази даних.

    Таблиці:
    - students:
        id (PK), student_name, student_id, age
    - teachers:
        id (PK), teacher_name, subject, city
    - groups:
        id (PK), group_name, group_id, start_date
    - gone_clients_content:
        id (PK), student_name, teacher_name, group_name, attendance_number, lost_reasons
        + зовнішні ключі (FK) на:
            students.id
            teachers.id
            groups.id

    Логіка:
    - Викликає open() для створення з’єднання.
    - Виконує CREATE TABLE IF NOT EXISTS для кожної таблиці.
    - Вмикає підтримку зовнішніх ключів (PRAGMA foreign_keys = on).
    - Після завершення закриває з’єднання (close()).

    """
    open()
    cursor.execute('''PRAGMA foreign_keys=on''')

    do('''CREATE TABLE IF NOT EXISTS students ( 
           id INTEGER PRIMARY KEY,
           student_name VARCHAR, 
           student_id INTEGER, 
           age INTEGER 
           )''')
    
    do('''CREATE TABLE IF NOT EXISTS teachers ( 
            id INTEGER PRIMARY KEY,
            teacher_name VARCHAR, 
            subject VARCHAR, 
            city VARCHAR)''')


    do('''CREATE TABLE IF NOT EXISTS groups (
               id INTEGER PRIMARY KEY,
               group_name VARCHAR, 
               group_id INTEGER,
               start_date DATE
               )''')


    do('''CREATE TABLE IF NOT EXISTS gone_clients_content (
            id INTEGER PRIMARY KEY,
            student_id INTEGER,
            teacher_id INTEGER,
            group_id INTEGER,
            attendance_number INTEGER,
            lost_reasons VARCHAR,
            FOREIGN KEY (student_id) REFERENCES students (id),
            FOREIGN KEY (teacher_id) REFERENCES teachers (id),
            FOREIGN KEY (group_id) REFERENCES groups (id)
        ) ''')
    close()

def insert_df_to_table(df, column_mapping, table_name):
    """
    Функція для вставки даних з DataFrame у таблицю SQLite.

    Аргументи:
    - df: pandas DataFrame з даними
    - column_mapping: словник {стовпець у DataFrame: стовпець у таблиці БД}
    - table_name: назва таблиці у базі даних

    Функціонал:
    1. Видаляє дублікатні рядки.
    2. Перейменовує колонки під назви таблиці.
    3. Перетворює NaN у None для вставки.
    4. Вставляє рядки через INSERT.
    5. Робить commit і закриває з’єднання.
    """
    
    # Видаляємо дублікати
    df_clean = df[list(column_mapping.keys())].drop_duplicates()
    # Перейменовуємо колонки
    df_clean = df_clean.rename(columns=column_mapping)

    # Відкриваємо з'єднання
    open()

    for _, row in df_clean.iterrows():
        values = [int(row[col]) if pd.notna(row[col]) and isinstance(row[col], (int, float)) else
                  (row[col] if pd.notna(row[col]) else None)
                  for col in column_mapping.values()]
        placeholders = ', '.join(['?'] * len(values))

        cursor.execute(f'''
            INSERT INTO {table_name} ({', '.join(column_mapping.values())})
            VALUES ({placeholders})
        ''', values)

    conn.commit()
    conn.close()
    print(f"✅ Таблицю '{table_name}' успішно заповнено.")

def unite_tables(df_main, df_teachers, missing_teachers=None, save_path=None):
    """
    Об'єднує дані про групи та викладачів у один DataFrame.

    Аргументи:
    - df_main: головний DataFrame з інформацією про групи (наприклад, відвідуваність)
    - df_teachers: DataFrame з інформацією про викладачів та групи
    - missing_teachers: словник {Назва групи: Викладач}, для ручних виправлень
    - save_path: шлях до CSV-файлу для збереження результату (якщо потрібно)

    Повертає:
    - df_merged: DataFrame з доданою колонкою 'Викладач'

    Логіка:
    1. Перейменовує колонку 'ID групи' у df_teachers на 'Назва групи' для злиття.
    2. Вибирає унікальні комбінації 'Назва групи' та 'Викладач'.
    3. З’єднує df_main і df_teachers по колонці 'Назва групи' методом left merge.
    4. Підставляє ручні виправлення з missing_teachers, якщо передано.
    5. Видаляє рядки без викладача.
    6. Зберігає результат у CSV, якщо save_path задано.
    """
   
    # Перейменовуємо колонку для злиття
    df_teachers = df_teachers.rename(columns={"ID групи": "Назва групи"})

    # Унікальні записи викладачів
    teachers_unique = df_teachers[["Назва групи", "Викладач"]].drop_duplicates()

    # Злиття з головним DataFrame
    df_merged = df_main.merge(teachers_unique, on="Назва групи", how="left")

    # Ручні виправлення викладачів
    if missing_teachers:
        for group_name, teacher in missing_teachers.items():
            df_merged.loc[df_merged["Назва групи"] == group_name, "Викладач"] = teacher

    # Видаляємо рядки без викладача
    df_merged = df_merged.dropna(subset=["Викладач"])

    # Зберігаємо у CSV, якщо потрібно
    if save_path:
        df_merged.to_csv(save_path, index=False)
        print(f"✅ Дані збережено у '{save_path}'")

    return df_merged


def insert_into_gone_clients_content(df, conn, missing_values=None):
    """
    Завантажує дані про студентів, групи та викладачів у таблицю gone_clients_content.

    Аргументи:
    - df: DataFrame, який містить щонайменше колонки:
        ['ПІБ студента', 'Назва групи', 'Викладач', 'Кількість відвіданих занять', 'Причина відвала']
    - conn: активне SQLite-з'єднання
    - missing_values: словник для ручного заповнення відсутніх значень (опціонально)

    Логіка:
    1. Для кожного рядка DataFrame:
        - Знаходить ID студента, викладача та групи у відповідних таблицях.
        - Пропускає рядки, якщо не знайдено жодного ID.
    2. Вставляє дані у gone_clients_content через INSERT:
        - student_name -> student_id
        - teacher_name -> teacher_id
        - group_name -> group_id
        - attendance_number та lost_reasons з DataFrame
    3. Робить commit після завершення вставки.
    """

    cursor = conn.cursor()

    for _, row in df.iterrows():
        student_name = row.get("ПІБ студента")
        group_name = row.get("Назва групи")
        teacher_name = row.get("Викладач")
        attendance_number = int(row.get("Кількість відвіданих занять", 0))
        lost_reason = row.get("Причина відвала", None)

        # Ручні виправлення, якщо передано
        if missing_values:
            student_name = missing_values.get("students", {}).get(student_name, student_name)
            group_name = missing_values.get("groups", {}).get(group_name, group_name)
            teacher_name = missing_values.get("teachers", {}).get(teacher_name, teacher_name)

        # Отримуємо ID зі студентів
        cursor.execute("SELECT id FROM students WHERE student_name = ?", (student_name,))
        student_result = cursor.fetchone()
        if not student_result:
            print(f"❌ Студента не знайдено: {student_name}")
            continue
        student_id = student_result[0]

        # Отримуємо ID викладача
        cursor.execute("SELECT id FROM teachers WHERE teacher_name = ?", (teacher_name,))
        teacher_result = cursor.fetchone()
        if not teacher_result:
            print(f"❌ Викладача не знайдено: {teacher_name}")
            continue
        teacher_id = teacher_result[0]

        # Отримуємо ID групи
        cursor.execute("SELECT id FROM groups WHERE group_name = ?", (group_name,))
        group_result = cursor.fetchone()
        if not group_result:
            print(f"❌ Групу не знайдено: {group_name}")
            continue
        group_id = group_result[0]

        # Вставка у таблицю gone_clients_content
        cursor.execute('''
                       INSERT INTO gone_clients_content (
                       student_id, teacher_id, group_id, attendance_number, lost_reasons)
                       VALUES (?, ?, ?, ?, ?)
                       ''', (student_id, teacher_id, group_id, attendance_number, lost_reason))        
    
    conn.commit()
    print("✅ Дані успішно додано в gone_clients_content")


def main():
    # --- Крок 1: Створюємо таблиці у базі ---
    create()

    # --- Крок 2: Завантажуємо дані у довідникові таблиці ---
    insert_df_to_table(
        df=df1,
        column_mapping={"ПІБ студента": "student_name",
                        "ID студента": "student_id",
                        "Вік дитини": "age"},
        table_name="students"
    )

    insert_df_to_table(
        df=df2,
        column_mapping={"Викладач": "teacher_name",
                        "Предмет": "subject",
                        "Місто": "city"},
        table_name="teachers"
    )

    insert_df_to_table(
        df=df1,
        column_mapping={"Назва групи": "group_name",
                        "ID групи": "group_id",
                        "Дата": "start_date"},
        table_name="groups"
    )

    # --- Крок 3: Об'єднання груп та викладачів ---
    missing_teachers_dict = {
        "215221_Умань_СБ_10:00 Геймдизайн": "Бондар Владислав"
    }

    df_merged = unite_tables(
        df_main=df1,
        df_teachers=df2,
        missing_teachers=missing_teachers_dict,
        save_path="data/1_Attended_classes_with_teachers.csv"
    )

    # --- Крок 4: Завантаження у gone_clients_content ---
    conn = sqlite3.connect(db_name)
    insert_into_gone_clients_content(df_merged, conn)
    conn.close()

    print("✅ ETL-процес завершено успішно!")



if __name__ == "__main__":
    main()