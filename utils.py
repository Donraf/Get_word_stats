import re
import copy
from collections import defaultdict

from constants import FILE_RUS_WORDS, ENCODING

rus_words_set = set()
with open(FILE_RUS_WORDS, 'r', encoding=ENCODING) as rus_word_file:
    for line in rus_word_file:
        rus_words_set.add(line.strip().lower())


def log_line_file(path, line):
    with open(path, 'a+', encoding=ENCODING) as file:
        file.write(line + '\n')


def sanitize_line(line):
    """
    Возвращает строку в нижнем регистре, очищенную от всех небуквенных символов, кроме разделителей
    (пробелов, переносов строки и т.п.)
    :param line: Строка
    :return:
    """
    return re.sub(r"[^А-Яа-яA-Za-z\s]", ' ', line.lower())


def get_words_dict(line):
    """
    Возвращает несортированный словарь вида
    {"слово": количество вхождений слова в строку}
    Пример:
    {
        "привет": 1,
            ...
        "я": 2,
        "осень": 7
    }
    :param line: Строка
    :return: Словарь
    """
    word_dict = defaultdict(int)
    processed_line = copy.copy(line)
    word_match = re.search(r"\w+", processed_line)
    while word_match:
        word = processed_line[word_match.start():word_match.end()]
        if word in rus_words_set:
            word_dict[word] += 1
        processed_line = processed_line[word_match.end():]
        word_match = re.search(r"\w+", processed_line)
    return word_dict
