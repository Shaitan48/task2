import pandas as pd
import psycopg2
from sqlalchemy import create_engine, MetaData, Table, String, Integer, Column, Text
import sqlalchemy as db

def process_data(engine):
    conn = engine.connect()

    metadata = db.MetaData()

    employee = Table('employees', metadata,
                     Column('id', Integer()),
                     Column('name', String(50)),
                     Column('age', Integer()),
                     Column('department', String(50))
                     )

    metadata.create_all(engine)

    d1 = {'id': 1, 'name': 'Alice', 'age': 30, 'department': 'HR'}
    d2 = {'id': 2, 'name': 'Bob', 'age': 25, 'department': 'Engineering'}
    d3 = {'id': 3, 'name': 'Charlie', 'age': 35, 'department': 'Sales'}

    df = pd.DataFrame.from_dict([d1, d2, d3])

    df.to_sql('employees', conn, if_exists='append', index=False)


    data = pd.read_sql('select * from employees', conn)

    #res = data.where(data['name'].apply(len) < 6).dropna(how='any').groupby('name').agg({'age': ['min', 'max']})

    return data

if __name__ == '__main__':
    db_user = 'postgres'
    db_pass = 'password'
    db_host = 'db'
    db_port = '5432'
    db_name = 'test_db'

    engine = create_engine(f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}')

    result = process_data(engine)

    print(result)