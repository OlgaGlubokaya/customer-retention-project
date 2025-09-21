"""
Назва файлу: loss_students_data_pipeline.py
Мета: 
1. Підготувати дані з CSV (виділити ID групи, знайти ID студентів через API).
2. Отримати статистику відвідуваності кожного студента (створити поле "Кількість відвіданих занять").
3. Доповнити датасет даними зі звіту о причинах відвалів (створити поля "Причина відвала" і "Вік дитини")
4. Зберегти результати в CSV для подальшого аналізу.

Вхідні дані:
    data/Raw_data.csv - таблиця з аналітичного додатку школи щодо відвалів,
    data/Lost_reasons.csv - таблиця з аналітичного додатку школи щодо причин відвалів,
    які озвучили батьки по проханню клієнтських менеджерів.

Вихідні результати:
    data/1_Attended_classes.csv
"""

import pandas as pd
import re
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# =============================
#   1. Підготовка даних
# =============================
def prepare_data(df):
    """
    Готує дані з таблиці груп:
    - Перейменовує колонку "ID групи" → "Назва групи".
    - Створює нову колонку "ID групи", витягуючи числовий ідентифікатор 
      із початку значення в полі "Назва групи".
      Якщо на початку немає числа — ставить None.

    Parameters
    ----------
    df : pandas.DataFrame
        Вхідний датафрейм з колонкою "ID групи".

    Returns
    -------
    pandas.DataFrame
        Оновлений датафрейм із двома полями:
        - "Назва групи" (оригінальне значення)
        - "ID групи" (виділений числовий ідентифікатор)
    """

    df.rename(columns={"ID групи": "Назва групи"}, inplace=True)
    df["ID групи"] = df["Назва групи"].apply(
        lambda x: int(re.match(r"^(\d+)", str(x)).group(0)) if re.match(r"^(\d+)", str(x)) else None
    )
    return df

# =============================
#   2. Отримання ID студентів
# =============================
def get_student_ids(df):
    """
    Отримує ID студентів з LMS LogikaSchool API та додає їх у датафрейм.

    Алгоритм:
    ---------
    1. Ініціалізує нову колонку "ID студента" зі значенням 0.
    2. Зчитує авторизаційні дані з .env (через os.getenv):
       - BACKEND_SESSION_ID
       - ACCESS_TOKEN
       - SERVER_ID
       - CREATED_TIMESTAMP
       - USER_ID
    3. Для кожного рядка у DataFrame:
       - Береться "ПІБ студента" та "ID групи".
       - Якщо немає ID групи → пропускає запис.
       - Виконує GET-запит до API:
         `https://lms.logikaschool.com/api/v2/group/student/index?groupId={group_id}`
       - Якщо відповідь успішна (200), витягує список студентів групи.
       - Шукає збіг ПІБ студента з API та додає знайдений "id"
         у колонку "ID студента".
       - Якщо збігу не знайдено → лишається 0.

    Parameters
    ----------
    df : pandas.DataFrame
        Датафрейм, який містить колонки:
        - "ПІБ студента"
        - "ID групи"

    Returns
    -------
    pandas.DataFrame
        Той самий датафрейм із додатковою/оновленою колонкою "ID студента".
        Якщо студент не знайдений або виникла помилка запиту — значення = 0.
    """
    df["ID студента"] = 0

    cookies = {
        "_backendMainSessionId": os.getenv("BACKEND_SESSION_ID"),
        "accessToken": os.getenv("ACCESS_TOKEN"),
        "SERVERID": os.getenv("SERVER_ID"),
        "createdTimestamp": os.getenv("CREATED_TIMESTAMP"),
        "userId": os.getenv("USER_ID")
    }

    headers = {
        "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    for i, row in df.iterrows():
        full_name = row["ПІБ студента"].strip()
        group_id = row["ID групи"]

        if pd.isna(group_id):
            print(f"⚠️ Пропускаємо {i} - немає ID групи")
            continue

        url = f"https://lms.logikaschool.com/api/v2/group/student/index?groupId={group_id}"

        try:
            response = requests.get(url, headers=headers, cookies=cookies)
            if response.status_code != 200:
                print(f"⚠️ Помилка {response.status_code} для групи {group_id}")
                continue
            data = response.json()
        except Exception as e:
            print(f"❌ Виняток для групи {group_id}: {e}")
            continue

        students = data.get("items") or data.get("data", {}).get("items") or []
        for student in students:
            student_name = student.get("fullName", "").strip().lower()
            if full_name.lower() in student_name or student_name in full_name.lower():
                df.at[i, "ID студента"] = student["id"]
                break

    return df

# =============================
#   3. Відвідуваність
# =============================
def get_attendance(df):
    """
    Отримує статистику відвідувань студентів через LMS LogikaSchool API 
    та додає їх у датафрейм.

    Алгоритм
    --------
    1. Додає нову колонку "Кількість відвіданих занять" зі значенням 0.
    2. Зчитує авторизаційні дані з .env (через os.getenv):
       - BACKEND_SESSION_ID
       - ACCESS_TOKEN
       - SERVER_ID
       - CREATED_TIMESTAMP
       - USER_ID
    3. Для кожного рядка у DataFrame:
       - Береться "ID студента" і "ID групи".
       - Якщо одного з них немає → пропускає рядок.
       - Виконує GET-запит до API:
         `https://lms.logikaschool.com/api/v1/stats/default/attendance?group={group_id}&students[]={student_id}`
       - Якщо відповідь успішна (200):
         * Парсить JSON.
         * Берe список занять і рахує, скільки з них мають `"status": "present"`.
       - Записує цю кількість у колонку "Кількість відвіданих занять".
       - Логує результат у консоль.

    Parameters
    ----------
    df : pandas.DataFrame
        Датафрейм, який містить колонки:
        - "ID студента"
        - "ID групи"
        - "ПІБ студента" (для логування виводу)

    Returns
    -------
    pandas.DataFrame
        Той самий датафрейм із додатковою/оновленою колонкою 
        "Кількість відвіданих занять".
        Якщо API не відповів або виникла помилка — значення залишається 0.
    """
    df["Кількість відвіданих занять"] = 0

    cookies = {
        "_backendMainSessionId": os.getenv("BACKEND_SESSION_ID"),
        "accessToken": os.getenv("ACCESS_TOKEN"),
        "SERVERID": os.getenv("SERVER_ID"),
        "createdTimestamp": os.getenv("CREATED_TIMESTAMP"),
        "userId": os.getenv("USER_ID")
    }

    headers = {
        "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

    for i, row in df.iterrows():
        student_id = row["ID студента"]
        group_id = row["ID групи"]

        if pd.isna(student_id) or pd.isna(group_id):
            print(f"⚠️ Пропущено: {row['ПІБ студента']} (рядок {i})")
            continue

        url = f"https://lms.logikaschool.com/api/v1/stats/default/attendance?group={int(group_id)}&students%5B%5D={int(student_id)}"

        try:
            response = requests.get(url, headers=headers, cookies=cookies)
            if response.status_code != 200:
                print(f"⚠️ Помилка {response.status_code} для студента {student_id}")
                continue
            data = response.json()
        except Exception as e:
            print(f"❌ Виняток для студента {student_id}: {e}")
            continue

        attendance = data.get("data", [{}])[0].get("attendance", [])
        count_present = sum(1 for lesson in attendance if lesson.get("status") == "present")
        df.at[i, "Кількість відвіданих занять"] = count_present

        print(f"✅ {row['ПІБ студента']}: {count_present} занять")

    return df
# =============================
#   4. Причини відвалів і вік дитини
# =============================

def add_fields(df1, df2, join_key):
    """
    Додає в df1 колонки 'Причина відвала' і 'Вік дитини' з df2.

    Parameters:
    -----------
    df1 : pd.DataFrame
        Основний датафрейм
    df2 : pd.DataFrame
        Датафрейм, з якого беремо додаткові поля
    join_key : str
        Назва колонки, по якій об’єднувати
    
    Returns:
    --------
    pd.DataFrame
        Новий датафрейм із доданими колонками
    """
    return df1.merge(
        df2[[join_key, "Причина відвала", "Вік дитини"]],
        on=join_key,
        how="left"
    )


# =============================
#   MAIN
# =============================
def main():
    
    df1 = pd.read_csv("data/Raw_data.csv")
    df2 = pd.read_csv("data/Lost_reasons.csv")
    df1 = prepare_data(df1)
    df1 = get_student_ids(df1)
    df1 = get_attendance(df1)
    df1 = add_fields(df1, df2, join_key="ПІБ студента")

    # Зберігаємо оновлені дані в датасет "1_Attended_classes.csv"
    df1.to_csv("data/1_Attended_classes.csv", index=False)
    
    print("🎉 Обробка завершена. Результати збережено у data/1_Attended_classes.csv")

if __name__ == "__main__":
    main()
