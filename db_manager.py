import psycopg2


class DBManager:
    def __init__(self, dbname, user, password, host, port):
        self.conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    def get_companies_and_vacancies_count(self):
        cur = self.conn.cursor()
        cur.execute('''
            SELECT c.name, COUNT(v.id)
            FROM companies AS c
            JOIN vacancies AS v ON c.id = v.company_id
            GROUP BY c.name;
        ''')
        return cur.fetchall()

    def get_all_vacancies(self):
        cur = self.conn.cursor()
        cur.execute('''
            SELECT c.name, v.name, v.salary_min, v.salary_max, v.url
            FROM vacancies AS v
            JOIN companies AS c ON v.company_id = c.id;
        ''')
        return cur.fetchall()

    def get_avg_salary(self):
        cur = self.conn.cursor()
        cur.execute('''
            SELECT AVG(salary_min + salary_max) / 2
            FROM vacancies;
        ''')
        return cur.fetchall()

    def get_vacancies_with_higher_salary(self):
        cur = self.conn.cursor()
        cur.execute(f'''
            SELECT c.name, v.name, v.salary_min, v.salary_max, v.url
            FROM vacancies AS v
            JOIN companies AS c ON v.company_id = c.id
            WHERE ((v.salary_min + v.salary_max) / 2) > {self.get_avg_salary()};
        ''')
        return cur.fetchall()

    def get_vacancies_with_keyword(self, keyword):
        cur = self.conn.cursor()
        cur.execute(f'''
            SELECT c.name, v.name, v.salary_min, v.salary_max, v.url
            FROM vacancies AS v
            JOIN companies AS c ON v.company_id = c.id
            WHERE v.name LIKE "%{keyword}%" OR c.name LIKE "%{keyword}%";
        ''')
        result = cur.fetchall()
        return result

