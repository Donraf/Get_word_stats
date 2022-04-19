import re
import copy
from collections import defaultdict


def log_line_file(path, line):
    with open(path, 'a+', encoding='utf-8') as file:
        file.write(line + '\n')


def pretty_print_dict(d):
    for key in d.keys():
        print(f"{key}: {d[key]}")


def sanitize_line(line):
    """
    Возвращает строку в нижнем регистре, очищенную от всех небуквенных символов, кроме разделителей (пробелов, переносов строки и т.п.)
    :param line: Строка
    :return:
    """
    return re.sub(r"[^А-Яа-яA-Za-z\s]", ' ', line.lower())


def get_words_dict(line):
    """
    Возвращает несортированный словарь вида
    {"слово": количество вхождений слова в строку}
    Пример:
    {"привет": 1,
     "я": 2,
     ...
    }
    :param line: Строка
    :return: Словарь
    """
    word_dict = defaultdict(int)
    processed_line = copy.copy(line)
    word = re.search(r"\w+", processed_line)
    while word:
        word_dict[processed_line[word.start():word.end()]] += 1
        processed_line = processed_line[word.end():]
        word = re.search(r"\w+", processed_line)
    return word_dict
