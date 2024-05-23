from sqlalchemy import (create_engine, Column, Integer, String,
                        insert, update, Sequence, Date, MetaData, delete)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import or_, and_
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import text


username = 'postgres'
db_password = 134472

db_url = f'postgresql+psycopg2://{username}:{db_password}@localhost:5432/new_hospitals'
engine = create_engine(db_url)

metadata = MetaData()
metadata.reflect(bind=engine)

connection = engine.connect()


def insert_row(table):
    values = {}

    for column_name in table.columns.keys():
        value = input(f'Введіть значення для стовпчика {column_name}: ')

        if value != '':
            values[column_name] = value

    query = insert(table).values(values)
    connection.execute(query)
    connection.commit()

    print('Everything is OK')


def update_data(table):
    print('назви стовпчиків')
    for column_name in table.columns.keys():
        print('\t', column_name)

    condition_column = input('введіть назву стовпчика для умови: ')
    condition_value = input('введіть значення для вказаного стовпчика для умови: ')

    # update where column==value
    values = {}

    for column_name in table.columns.keys():
        value = input(f'Введіть значення для стовпчика {column_name}: ')

        if value != '':
            values[column_name] = value

    column = getattr(table.c, condition_column)

    query = update(table) \
            .where(column == column.type.python_type(condition_value)) \
            .values(values)

    connection.execute(query)
    connection.commit()


def delete_data(table):
    print('назви стовпчиків')
    for column_name in table.columns.keys():
        print('\t', column_name)

    # condition_column = input('введіть назву стовпчика для умови: ')
    # condition_value = input('введіть значення для вказаного стовпчика для умови: ')
    #
    # column = getattr(table.c, condition_column)
    #
    # query = delete(table).where(column == column.type.python_type(condition_value))

    condition = input('введіть умову для одного стовпчика')
    # premium < 20

    query = delete(table).where(eval('table.c.' + condition))

    connection.execute(query)
    connection.commit()

    print("Column names")
    for column_name in table.columns.keys():
        print("\t", column_name)

    # condition_column = input("Input table name for condition: ")
    # condition_value = input("Input value for table: ")

    # column = getattr(table.c, condition_column)
    #
    # query = delete

    condition = input("Input condition for one column: ")
    # premium > 80

    query = delete(table).where(eval("table.c" + condition))

    connection.execute(query)
    connection.commit()


while True:
    print("Tables from database: ")
    for table_name in metadata.tables.keys():
        print(table_name)

    table_name = input("Input table name: ")

    if table_name in metadata.tables:
        table = metadata.tables[table_name]

        print("Choose function")
        print("1 - add data")
        print("2 - delete data")
        print("3 - change data")

        command = int(input(""))
        if command == 1:
            insert_row(table)
        elif command == 2:
            delete_data(table)
        elif command == 3:
            update_data(table)


