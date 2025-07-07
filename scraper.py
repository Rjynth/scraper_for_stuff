import re
import time
import random
import sqlite3
import logging
from urllib.parse import urljoin
from urllib.robotparser import RobotFileParser
import requests
from bs4 import BeautifulSoup

# Конфигурация
BASE_URL = 'https://www.eurobike.com/frankfurt/de.html'
DB_PATH = 'eurobike.db'
HEADERS = {'User-Agent': 'Mozilla/5.0 (compatible; EurobikeScraper/1.0)'}
DELAY_MIN, DELAY_MAX = 1, 3  # секунды между запросами

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')


def check_robots():
    """Проверяем доступность скрапинга в robots.txt"""
    rp = RobotFileParser()
    robots_url = urljoin(BASE_URL, '/robots.txt')
    rp.set_url(robots_url)
    rp.read()
    allowed = rp.can_fetch(HEADERS['User-Agent'], BASE_URL)
    if not allowed:
        logging.error('Скрейпинг запрещён robots.txt')
        raise SystemExit('Скрейпинг запрещён.')
    logging.info('Скрейпинг разрешён.')


def init_db():
    """Создаём базу и таблицу participants"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            description TEXT,
            country TEXT,
            website TEXT,
            email TEXT,
            phone TEXT
        )
    ''')
    conn.commit()
    return conn


def extract_contacts(text):
    """Извлекаем email и телефон из произвольного текста"""
    email = None
    phone = None
    # простая regex для email
    m = re.search(r'[\w\.-]+@[\w\.-]+', text)
    if m:
        email = m.group(0)
    # простая regex для телефона (международный формат)
    m2 = re.search(r'\+?\d[\d\s\-()]{5,}\d', text)
    if m2:
        phone = m2.group(0)
    return email, phone


def parse_participant(block):
    """Парсим один блок участника"""
    # предполагаем, что блок block — тег <div class="participant-item"> и т.д.
    name = block.find('h2').get_text(strip=True) if block.find('h2') else None
    desc = block.find('p', class_='description')
    description = desc.get_text(strip=True) if desc else None
    country = block.find('span', class_='country')
    country = country.get_text(strip=True) if country else None
    site_link = block.find('a', href=True)
    website = site_link['href'].strip() if site_link else None
    # соберём текст всего блока для поиска контактов
    text = block.get_text(separator=' ', strip=True)
    email, phone = extract_contacts(text)
    return {
        'name': name,
        'description': description,
        'country': country,
        'website': website,
        'email': email,
        'phone': phone
    }


def save_participant(conn, data):
    """Сохраняем словарь data в БД"""
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO participants (name, description, country, website, email, phone)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (data['name'], data['description'], data['country'],
          data['website'], data['email'], data['phone']))
    conn.commit()


def scrape():
    check_robots()
    conn = init_db()
    # Загружаем стартовую страницу
    resp = requests.get(BASE_URL, headers=HEADERS)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'lxml')
    # Находим все блоки участников
    items = soup.select('div.participant-item')
    if not items:
        logging.warning('Не найдено участников на странице.')
    for block in items:
        try:
            data = parse_participant(block)
            save_participant(conn, data)
            logging.info(f"Сохранён: {data['name']}")
        except Exception as e:
            logging.error(f"Ошибка при парсинге блока: {e}")
        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
    conn.close()
    logging.info('Скрейпинг завершён.')


if __name__ == '__main__':
    scrape()