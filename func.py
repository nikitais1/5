from typing import Any

import requests  # Для запросов по API


def get_info(id_companies: dict) -> list[dict[str, Any]]:
    """Получаем информацию о компаниях и формируем ее в словарь"""
    company_vacancy = []
    try:
        for id_company in id_companies:
            url_hh = f'https://api.hh.ru/employers/{id_company}'
            info = requests.get(url_hh).json()
            company_vacancy.append({'company_id': info['id'],
                                    'company_name': info['name'],
                                    'description': info['description'],
                                    'url': info['vacancies_url']
                                    })
    except KeyError:
        print("По данным критериям не нашлось вакансий")
    return company_vacancy


def get_vacancy(url: Any) -> list[dict[str, int | float]]:
    """Получаем информацию о вакансиях, используя url от компании.
            Полученную информацию формируем в словарь"""
    info_vacancy = []
    info = requests.get(url).json()['items']
    salary_max = 0
    try:
        for vacancy in info:
            if vacancy['salary']:
                salary_from = vacancy['salary']['from'] if vacancy['salary']['from'] else 0
                salary_to = vacancy['salary']['to'] if vacancy['salary']['to'] else 0
                salary_avr = (salary_from + salary_to) / 2
                if salary_from > salary_max:
                    salary_max = salary_from
                elif salary_to > salary_max:
                    salary_max = salary_to
            else:
                salary_from = 0
                salary_to = 0
                salary_avr = 0
                salary_max = 0
            info_vacancy.append({'id_vacancy': vacancy['id'],
                                 'id_company': vacancy['employer']['id'],
                                 'name': vacancy['name'],
                                 'url': vacancy['area']['url'],
                                 'salary_from': salary_from,
                                 'salary_to': salary_to,
                                 'salary_avr': salary_avr,
                                 'salary_max': salary_max,
                                 'area': vacancy['area']['name']
                                 })
    except KeyError:
        print("Неправильные критерии поиска")
    return info_vacancy


def delete_symbol(text: str) -> str:
    """Метод для удаления ненужных символов в тексте"""
    symbols = ['\n', '<strong>', '</strong>', '</p>', '<p>',
               '<b>', '</b>', '<ul>', '<br />', '</ul>', '&nbsp', '</li>', '</ul>',
               '&laquo', '&ndash', '&mdash', '<em>', '&middot', '</em>', '&raquo']
    for symbol in symbols:
        description = text.replace(symbol, '')
    return description
