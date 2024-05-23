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

Session = sessionmaker(bind=engine)
session = Session()

# assign own names to columns
docs = metadata.tables['doctors']
specs = metadata.tables['specializations']
docsspecs = metadata.tables['doctorsspecializations']
vacations = metadata.tables["vacations"]

wards = metadata.tables["wards"]
deps = metadata.tables["departments"]

dons = metadata.tables["donations"]

spons = metadata.tables["sponsors"]



def report_doctor_specializations():
    result = session.query(docs.c.name.label('doctorname'),
                           docs.c.surname) \
             .join(docsspecs, docsspecs.c.doctor_id == docs.c.id) \
             .join(specs, docsspecs.c.specialization_id == specs.c.id).all()

    if result:
        for row in result:
            print(f"{row.doctorname} {row.surname} with specialization {row.specialization_name}")

def  report_doctors_salary_not_on_vacation():
    result = session.query(docs.c.surname,
                           docs.c.salary) \
        .join(vacations, vacations.c.id == docs.c.id)

    if result:
        for row in result:
            print(f"{row.surname} with salary {row.salary} on vacation.")


def report_wards_depatment():
    result = (session.query(
        wards.c.name.label('ward_name'),
        deps.c.name.label('dep_name'))
              .join(deps, wards.c.department_id == deps.c.id)
              .all())

    if result:
        for row in result:
            print(f"Ward {row.ward_name} belongs to {row.dep_name}")


def repor_donation_of_last_mounth():
    result = (session.query(
                dons.c.donation_date,
                deps.c.name.label('dep_name'),
                dons.c.donation_amount)
              .join(spons, dons.c.sponsor_id == spons.c.sponsor_id)
              .join(deps, dons.c.department_id == deps.c.id)
              .all())

    if result:
        for row in result:
            print(f"Summa of {row.donation_amount} was donated on {row.donation_date} to {row.dep_name} from {row.sponsor_name}")


def  report_departaments_donation():
    ...





while True:
    print("1. Вивести повні імена лікарів та їх спеціалізації")
    print("2. Вивести прізвища та зарплати лікарів, які не перебувають у відпустці")
    print("3. Вивести назви палат, які знаходяться у певному відділенні;")
    print("4. Вивести усі пожертвування за вказаний місяць у вигляді: відділення, спонсор, сума пожертвування, дата пожертвування;")
    print("5. Вивести назви відділень без повторень, які спонсоруються певною компанією.")

    print("0. Вийти")
    choice = input("Оберіть опцію: ")

    if choice == "1":
        report_doctor_specializations()
    elif choice == "2":
        report_doctors_salary_not_on_vacation()
    elif choice == "3":
        report_wards_depatment()
    elif choice == "4":
        repor_donation_of_last_mounth()
    elif choice == "5":
        report_departaments_donation()

    elif choice == "0":
        break
    else:
        print("Невірний вибір. Будь ласка, оберіть знову.")

session.close()