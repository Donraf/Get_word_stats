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
        "я": 7,
        "осень": 2
    }
    :param line: Строка
    :return: Словарь
    """
    word_dict = defaultdict(int)
    word_match = re.findall(r"\w+", line)
    for word in word_match:
        if word in rus_words_set:
            word_dict[word] += 1
    return word_dict
