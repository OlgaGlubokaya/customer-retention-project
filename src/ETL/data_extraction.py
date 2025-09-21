"""
Назва файлу: data_extraction.py

Мета:
Екстракція та об'єднання даних із локальної SQLite-бази
для подальшого аналізу у форматі CSV.

Призначення модуля
------------------
1. Підключення до локальної бази даних `Gone_Clients.sqlite`.
2. Виконання SQL-запиту з JOIN'ами між таблицями:
   - students
   - teachers
   - groups
   - gone_clients_content
3. Формування повного DataFrame з усіма ключовими атрибутами:
   - Інформація про студентів: ім'я, ID, вік
   - Інформація про викладачів: ім'я, предмет, місто
   - Інформація про групи: назва, ID, дата початку
   - Факти відходу: кількість відвіданих занять та причина відвала
4. Збереження результату у CSV-файл для подальшого аналізу чи візуалізації.

Особливості
------------
- Використовується `pandas.read_sql_query` для прямого завантаження SQL-запиту у DataFrame.
- CSV зберігається з кодуванням 'utf-8-sig' для сумісності з Excel.
- Код демонструє навички:
  - витягування даних з SQLite
  - об'єднання кількох таблиць через JOIN
  - конвертацію у формат CSV для аналітики

Вхідний файл
-------------  
- data/Gone_Clients.sqlite - база даних клієнтів, які припинили навчання в школі.

Вихідний файл
-------------
- `data/Final_Gone_Clients_Report.csv` — готовий для аналізу файл з усіма об’єднаними даними про студентів, групи, викладачів та відходи.
"""

import sqlite3
import pandas as pd

# Підключення до бази даних
conn = sqlite3.connect("data/Gone_Clients.sqlite")

# SQL-запит до всіх таблиць з JOIN'ами
query = '''
SELECT 
    s.student_name,
    s.student_id,
    s.age,
    t.teacher_name,
    t.subject,
    t.city,
    g.group_name,
    g.group_id,
    g.start_date,
    gcc.attendance_number,
    gcc.lost_reasons
FROM gone_clients_content AS gcc
JOIN students AS s ON gcc.student_id= s.id
JOIN teachers AS t ON gcc.teacher_id = t.id
JOIN groups AS g ON gcc.group_id = g.id
'''

# Завантажуємо результат у DataFrame
df_full = pd.read_sql_query(query, conn)

# Зберігаємо у CSV
df_full.to_csv("data/Final_Gone_Clients_Report.csv", index=False, encoding='utf-8-sig')

# Закриваємо з'єднання
conn.close()

print("✅ CSV з усіма об'єднаними даними збережено як Final_Gone_Clients_Report.csv")
