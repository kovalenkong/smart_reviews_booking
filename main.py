import datetime
import logging
import sqlite3
from typing import List

import bs4
import requests
import yaml
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(_handler)


class Review:
    _months = {
        'Январь': 1,
        'Февраль': 2,
        'Март': 3,
        'Апрель': 4,
        'Май': 5,
        'Июнь': 6,
        'Июль': 7,
        'Август': 8,
        'Сентябрь': 9,
        'Октябрь': 10,
        'Ноябрь': 11,
        'Декабрь': 12,
    }

    def __init__(
            self, hotel_name: str, date: str, title: str, country: str, positive_review: str,
            negative_review: str, score: str
    ):
        self.hotel_name = hotel_name
        self.date = self.parse_date(date)
        self.title = title
        self.country = country
        self.positive_review = positive_review
        self.negative_review = negative_review
        self.score = score

    @classmethod
    def from_tag(cls, hotel_name: str, tag: bs4.Tag):
        date = tag.find_next('span', class_='c-review-block__date').text.strip()

        title = None
        title_tag = tag.find_next('h3', class_='c-review-block__title')
        if title_tag is not None:
            title = title_tag.text.strip()

        country = None
        country_tag = tag.find('span', class_='bui-avatar-block__subtitle')
        if country_tag is not None:
            country = country_tag.text.strip()

        positive_review, negative_review = None, None
        positive_tag = tag.find('span', class_='positive')
        if positive_tag is not None:
            positive_review = positive_tag.find_previous_sibling('span').text.strip()
        if positive_review is None:
            russian_positive_tag: bs4.Tag = tag.find('span', class_='bui-u-sr-only', text='Понравилось')
            if russian_positive_tag is not None:
                positive_review = russian_positive_tag.parent.find_next_sibling('span',
                                                                                class_='c-review__body').text.strip()

        negative_tag = tag.find('span', class_='negative')
        if negative_tag is not None:
            negative_review = negative_tag.find_previous_sibling('span').text.strip()
        if negative_review is None:
            russian_negative_tag: bs4.Tag = tag.find('span', class_='bui-u-sr-only', text='Не понравилось')
            if russian_negative_tag is not None:
                negative_review = russian_negative_tag.parent.find_next_sibling('span',
                                                                                class_='c-review__body').text.strip()
        score = tag.find('div', class_='bui-review-score__badge').text.strip()
        return cls(hotel_name, date, title, country, positive_review, negative_review, score)

    def parse_date(self, date: str) -> datetime.date:
        month, year = date.split(' ')
        return datetime.date(int(year), self._months[month], 1)

    def __repr__(self):
        return '{} ({}): {}; Positive: {}, Negative: {}'.format(
            self.date, self.country,
            self.title, self.positive_review, self.negative_review
        )


def parse_reviews(page: str, hotel_name: str) -> List[Review]:
    soup = BeautifulSoup(page, 'html.parser')
    reviews = soup.find_all('div', class_='c-review-block')
    return [Review.from_tag(hotel_name, i) for i in reviews]


def insert_reviews(cursor: sqlite3.Cursor, reviews: List[Review]):
    for i in reviews:
        cursor.execute(
            '''
            INSERT INTO reviews(hotel, date, title, country, positive_review, negative_review, score) 
            VALUES (?, ?, ?, ?, ?, ?, ?);''',
            (i.hotel_name, i.date, i.title, i.country, i.positive_review, i.negative_review, i.score)
        )


def get_reviews(session: requests.Session, offset: int, hotel_name: str, pagename: str, id_: str, cc1: str):
    resp = session.get(
        'https://www.booking.com/reviewlist.ru.html',
        params={
            'sid': 'bbecb9f9eb3a6acdbbca99bb6b8219c5',
            'cc1': cc1,
            'dist': '1',
            'pagename': pagename,
            'sort': 'f_recent_desc',
            'type': 'total',
            'offset': offset,
            'rows': '10',
            '_': id_,
        },
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.60 Safari/537.36'
        }
    )
    if not resp.ok:
        raise Exception(f'got status {resp.status_code}; offset {offset}')
    return parse_reviews(resp.text, hotel_name)


def main():
    with open('params.yaml') as f:
        params = yaml.load(f.read(), Loader=yaml.CLoader)

    conn = sqlite3.connect('reviews.db')
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS reviews(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        hotel TEXT,
        date DATE,
        title TEXT,
        country TEXT,
        positive_review TEXT NULL,
        negative_review TEXT NULL,
        tr_positive_review REAL NULL,
        tr_negative_review REAL NULL,
        score REAL
    );
    ''')
    conn.commit()
    with requests.session() as s:
        for hotel_params in params:
            pagename = hotel_params['pagename']
            id_ = hotel_params['id']
            hotel_name = hotel_params['hotel_name']
            cc1 = hotel_params.get('cc1', 'ru')
            from_ = hotel_params['from']
            to = hotel_params['to']
            logger.info(f'parseing hotel {hotel_name}')
            for i in range(from_, to, 10):
                logger.debug(f'parsing offset {i}')
                reviews = get_reviews(s, i, hotel_name, pagename, id_, cc1)
                insert_reviews(cursor, reviews)
                conn.commit()
                logger.info(f'offset {i} done')
        logger.info(f'hotel {hotel_name} done')


if __name__ == '__main__':
    main()
