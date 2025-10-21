import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import pandas as pd

# Формат подключения: mysql+pymysql://USER:PASSWORD@HOST:PORT/DBNAME
DB_USER = "j30084097_tt"
DB_PASSWORD = "1654790349fd"
DB_HOST = "mysql.65e3ab49565f.hosting.myjino.ru"
DB_PORT = "3306"
DB_NAME = "j30084097_tregubova_teterkin_pz4"

DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, echo=True)

SessionLocal = sessionmaker(bind=engine)

# Проверка подключения
with engine.connect() as conn:
    result = conn.execute(text("SELECT NOW();"))
    print("Текущее время на сервере:", result.fetchone())

# Create
def create_table_from_df(df, table_name, engine, primary_key=None, if_not_exists=True):
    type_map = {
        'int64': 'INT',
        'float64': 'FLOAT',
        'object': 'TEXT'
    }
    cols = []
    for col, dtype in df.dtypes.items():
        col_def = f"`{col}` {type_map.get(str(dtype), 'TEXT')}"
        if primary_key and col == primary_key:
            col_def += " PRIMARY KEY"
        cols.append(col_def)
    clause = "IF NOT EXISTS " if if_not_exists else ""
    ddl = f"CREATE TABLE {clause}`{table_name}` ({','.join(cols)});"
    with engine.begin() as conn:
        conn.execute(text(ddl))

# SELECT
def select_all(table_name, engine, limit=10):
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {table_name} LIMIT {limit}"), conn)
        return df

# INSERT (одна строка/словарь)
def insert_row(table_name, data_dict, engine):
    cols = ",".join(data_dict.keys())
    placeholders = ",".join([f":{c}" for c in data_dict.keys()])
    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
    with engine.begin() as conn:
        conn.execute(text(sql), data_dict)

# UPDATE (по условиям)
def update_row(table_name, set_dict, where_dict, engine):
    set_clause = ", ".join([f"{k}=:{k}" for k in set_dict.keys()])
    where_clause = " AND ".join([f"{k}=:{k}_w" for k in where_dict.keys()])
    params = {**set_dict, **{k + "_w": v for k, v in where_dict.items()}}
    sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
    with engine.begin() as conn:
        conn.execute(text(sql), params)

# DELETE (по условиям)
def delete_row(table_name, where_dict, engine):
    where_clause = " AND ".join([f"{k}=:{k}" for k in where_dict.keys()])
    sql = f"DELETE FROM {table_name} WHERE {where_clause};"
    with engine.begin() as conn:
        conn.execute(text(sql), where_dict)

# DROP таблицу
def drop_table(table_name, engine):
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))

# Экспорт в csv
def export_table_to_csv(table_name, filename, engine):
    with engine.connect() as conn:
        df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)
    df.to_csv(filename, index=False)

# CREATE
test_df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Иван', 'Петр', 'Мария'],
    'age': [20, 21, 19],
    'group_name': ['ИТ-101', 'ИТ-101', 'ИТ-102']
})

# ОЧИСТКА ПЕРЕД НАЧАЛОМ
with engine.begin() as conn:
    conn.execute(text("DROP TABLE IF EXISTS students;"))

create_table_from_df(test_df, "students", engine)
create_table_from_df(test_df, "students", engine)

# INSERT начальных данных
insert_row("students", {"id": 1, "name": "Иван Иванов", "age": 20, "group_name": "ИТ-101"}, engine)
insert_row("students", {"id": 2, "name": "Петр Петров", "age": 21, "group_name": "ИТ-101"}, engine)
insert_row("students", {"id": 3, "name": "Мария Сидорова", "age": 19, "group_name": "ИТ-102"}, engine)

# INSERT дополнительной записи
insert_row("students", {"id": 4, "name": "Анна", "age": 20, "group_name": "ИТ-102"}, engine)

# UPDATE
update_row("students", {"age": 22}, {"id": 1}, engine)

# DELETE
delete_row("students", {"id": 4}, engine)

# EXPORT
export_table_to_csv("students", "students_export.csv", engine)

# DROP
drop_table("students", engine)