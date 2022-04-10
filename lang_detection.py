import sqlite3
from collections import defaultdict

import fasttext
from transformers import pipeline


class Translator:
    def __init__(self, fasttext_model_path: str):
        self._translators = {}
        self._fasttext_model = fasttext.load_model(fasttext_model_path)

    def detect_language(self, text: str):
        labels = self._fasttext_model.predict(text.replace('\n', ' '))[0]
        if len(labels) != 1:
            raise Exception(f'got len {len(labels)}')
        return labels[0][9:]

    def translate(self, text: str, to: str):
        from_lang = self.detect_language(text)
        model_name = f'Helsinki-NLP/opus-mt-{from_lang}-{to}'
        if model_name not in self._translators:
            self._translators[model_name] = pipeline(f'translation_{from_lang}_to_{to}', model=model_name)
        return self._translators[model_name](text)[0]['translation_text']


def get_lang(ft_model: fasttext.FastText._FastText, text: str) -> str:
    labels = ft_model.predict(text.replace('\n', ' '))[0]
    if len(labels) != 1:
        raise Exception(f'got labels >1 ({len(labels)})')
    return labels[0][9:]


def get_languages() -> dict:
    conn = sqlite3.connect('reviews.db')
    cursor = conn.cursor()
    ft_model = fasttext.load_model('lid.176.bin')
    cursor.execute('''
    SELECT id, positive_review, negative_review FROM reviews
    WHERE positive_review IS NOT NULL OR negative_review IS NOT NULL; 
    ''')
    rows = cursor.fetchall()
    langs = defaultdict(int)
    for row in rows:
        id_, positive_review, negative_review = row
        lang_code = None
        if positive_review is not None:
            lang_code = get_lang(ft_model, positive_review)
        elif negative_review is not None:
            lang_code = get_lang(ft_model, negative_review)
        if lang_code is not None:
            langs[lang_code] += 1
    return langs


def main():
    conn = sqlite3.connect('reviews.db')
    cursor = conn.cursor()
    translator = Translator('lid.176.bin')

    common_langs = ', '.join(f"'{i[0]}'" for i in list(filter(lambda i: i[1] > 25, get_languages().items())))
    cursor.execute('''
    SELECT id, positive_review, negative_review FROM reviews
    WHERE (positive_review IS NOT NULL OR negative_review IS NOT NULL)
    AND (lang_code NOT IN ({}))
    '''.format(common_langs))
    rows = cursor.fetchall()
    for row in rows:
        id_, positive_review, negative_review = row
        if positive_review is not None:
            tr_positive_review = translator.translate(positive_review, 'en')
            cursor.execute('UPDATE reviews SET tr_positive_review=? WHERE id=?;', (tr_positive_review, id_))
        if negative_review is not None:
            tr_negative_review = translator.translate(negative_review, 'en')
            cursor.execute('UPDATE reviews SET tr_negative_review=? WHERE id=?;', (tr_negative_review, id_))
        conn.commit()


if __name__ == '__main__':
    main()
