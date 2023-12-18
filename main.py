from config import config
from db_manager import DBManager


def main() -> None:
    params = config()
    db = DBManager('jo', params)
    db.connect_db()
    db.create_tables()
    db.write_info_in_table()

    user_answer = int(input("""
Выбери данные, которые хочешь получить:
1 - список всех компаний и количество вакансий у каждой компании.
2 - список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
3 - среднюю зарплату по вакансиям.
4 - список всех вакансий, у которых зарплата выше средней по всем вакансиям.
5 - список всех вакансий, в названии которых содержатся переданные в метод слова, например python.\n
"""))

    if user_answer == 1:
        db.get_companies_and_vacancies_count()
    elif user_answer == 2:
        db.get_all_vacancies()
    elif user_answer == 3:
        db.get_avg_salary()
    elif user_answer == 4:
        db.get_vacancies_with_higher_salary()
    elif user_answer == 5:
        word = str(input('Введите ключевое слово\n'))
        db.get_vacancies_with_keyword(word)
    else:
        print('Нет такой операции')


if __name__ == '__main__':
    main()
