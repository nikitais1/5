from typing import Any
import psycopg2
from func import get_info, delete_symbol, get_vacancy


class DBManager:
    """Класс для работы с БД"""

    def __init__(self, db_name: Any, params: Any) -> None:
        self.db_name = db_name  # название базы данных
        self.params = params  # параметры подключение, получаем через config
        self.id_companies = [
            1740,  # Яндекс
            78638,  # Тинькофф
            3529,  # СБЕР
            39305,  # Газпром нефть
            4181,  # Банк ВТБ
            80,  # Альфа-Банк
            1942330,  # Пятёрочка
            2180,  # Ozon
            2748,  # Ростелеком
            3776  # МТС
        ]

    def connect_db(self) -> None:
        """Метод для подключения и создания БД"""

        connection = psycopg2.connect(database='postgres', **self.params)
        connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.db_name}")
            cursor.execute(f"CREATE DATABASE {self.db_name}")
        except psycopg2.ProgrammingError:
            print("Не удалось создать базу данных")

        cursor.close()
        connection.close()

    def create_tables(self) -> None:
        """Метод для создания таблиц с компаниями и вакансиями"""

        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cursor:
            try:
                cursor.execute("""
                           CREATE TABLE companies (
                           company_id int PRIMARY KEY,
                           company_name varchar(50) NOT NULL,
                           description text
                           );
                           """)

                cursor.execute("""
                            CREATE TABLE vacancies (
                            id_vacancy int PRIMARY KEY,
                            company_id int REFERENCES companies(company_id) NOT NULL,
                            vacancy_name varchar(100) NOT NULL,
                            url varchar(100) NOT NULL,
                            salary_from varchar(100), 
                            salary_to varchar(100),
                            salary_avr varchar(100),
                            salary_max varchar(100),
                            area varchar(100)
                            );
                            """)

            except psycopg2.ProgrammingError:
                print("Не удалось создать таблицы")
        connection.commit()
        connection.close()

    def write_info_in_table(self) -> None:
        """Метод для заполнения информацией таблиц компаний и вакансий.
              Используются функции get_info, get_vacancy"""

        connection = psycopg2.connect(database=self.db_name, **self.params)
        hh = get_info(self.id_companies)
        with connection.cursor() as cursor:
            for i in range(len(hh)):
                hh_replace = delete_symbol(hh[i]['description'])  # удаляем ненужные символы из текста
                hh_gv = get_vacancy(hh[i]['url'])
                cursor.execute("""INSERT INTO companies VALUES (%s,%s,%s)""",
                               (hh[i]['company_id'], hh[i]['company_name'], hh_replace))
                for count in range(len(hh_gv)):
                    cursor.execute("""INSERT INTO vacancies VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s )""",
                                   (hh_gv[count]['id_vacancy'], hh_gv[count]['id_company'],
                                    hh_gv[count]['name'], hh_gv[count]['url'],
                                    hh_gv[count]['salary_from'], hh_gv[count]['salary_to'],
                                    hh_gv[count]['salary_avr'], hh_gv[count]['salary_max'],
                                    hh_gv[count]['area'],
                                    ))

        connection.commit()
        connection.close()

    def get_companies_and_vacancies_count(self) -> None:
        """Метод для получения списка всех компаний и количество вакансий у каждой компании."""
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cur:
            cur.execute('''
                SELECT c.company_name, COUNT(v.id_vacancy)
                FROM companies AS c
                JOIN vacancies AS v ON c.company_id = v.company_id
                GROUP BY c.company_name;
            ''')
            rows = cur.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()

    def get_all_vacancies(self) -> None:
        """Метод для получения списка всех вакансий с указанием названия компании,
            названия вакансии и зарплаты и ссылки на вакансию."""
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cur:
            cur.execute('''
                SELECT c.company_name, v.*
                FROM vacancies AS v
                JOIN companies AS c ON v.company_id = c.company_id;
            ''')
            rows = cur.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()

    def get_avg_salary(self) -> None:
        """Метод для получения средней зарплаты по вакансиям."""
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cur:
            cur.execute('''
                SELECT vacancy_name,salary_avr
                from vacancies 
                ORDER BY salary_avr DESC
            ''')
            rows = cur.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()

    def get_vacancies_with_higher_salary(self) -> None:
        """Метод для получения списка всех вакансий, у которых зарплата выше средней по всем вакансиям."""
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cur:
            cur.execute(f'''
                SELECT c.company_name, v.*
                FROM vacancies AS v
                JOIN companies AS c ON v.company_id = c.company_id
                WHERE (v.salary_max > salary_avr)
                ORDER BY salary_max;
            ''')
            rows = cur.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()

    def get_vacancies_with_keyword(self, keyword: dict) -> None:
        """Метод для получения списка всех вакансий, в названии которых содержатся переданные в метод слова"""
        connection = psycopg2.connect(database=self.db_name, **self.params)
        with connection.cursor() as cur:
            cur.execute(f'''
                SELECT c.company_name, v.*
                FROM vacancies AS v
                JOIN companies AS c ON v.company_id = c.company_id
                WHERE v.vacancy_name LIKE '%{keyword.title().strip()}%';
            ''')
            rows = cur.fetchall()
            for row in rows:
                print(row)

        connection.commit()
        connection.close()
