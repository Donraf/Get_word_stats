import re
import os
import time
from collections import defaultdict
from operator import itemgetter

from constants import *
from utils import log_line_file


class CsvHandler:
    def _check_for_duplicate(self, csv_file, words_in_doc):
        words = []
        for line_num, line in enumerate(csv_file):
            word_csv = re.match(r'\w+', line).group()
            if word_csv in words_in_doc:
                words.append({
                    WORD: word_csv,
                    WORD_NUM_IN_DOC: words_in_doc[word_csv],
                    FIRST_OCCURRENCE: False,
                    WORD_NUM_LINE: line_num,
                })
                del words_in_doc[word_csv]
        for key in words_in_doc.keys():
            words.append({
                WORD: key,
                WORD_NUM_IN_DOC: words_in_doc[key],
                FIRST_OCCURRENCE: True,
                WORD_NUM_LINE: -1,
            })
        return words

    def add_dict(self, csv_path, words_in_doc: defaultdict):
        start_time = time.time()

        with open(csv_path, "r", encoding=ENCODING) as csv_file:
            words = self._check_for_duplicate(csv_file, words_in_doc)
        words = sorted(words, key=itemgetter(FIRST_OCCURRENCE, WORD_NUM_LINE, WORD_NUM_IN_DOC))

        end_search_time = time.time()

        self._add_record(csv_path, words)

        end_write_time = time.time()

        return end_search_time - start_time, end_write_time - end_search_time

    def _add_record(self, csv_path, words):
        new_csv_path = os.path.join(TEMP_CSV_DIR, r"temp_csv.txt")

        with open(csv_path, "r", encoding=ENCODING) as csv_file, \
                open(new_csv_path, "w+", encoding=ENCODING) as new_csv_file:
            word_index = 0
            for line_num, line in enumerate(csv_file):
                if word_index != len(words) and \
                    not words[word_index][FIRST_OCCURRENCE] and \
                        words[word_index][WORD_NUM_LINE] == line_num:
                    num_csv = re.search(r"\d+", line).group()
                    new_csv_file.write(f"{words[word_index][WORD]}" + CSV_DELIMITER +
                                       f"{words[word_index][WORD_NUM_IN_DOC] + int(num_csv)}\n")
                    word_index += 1
                else:
                    new_csv_file.write(line)
            for index in range(word_index, len(words)):
                new_csv_file.write(f"{words[index][WORD]}" + CSV_DELIMITER +
                                   f"{words[index][WORD_NUM_IN_DOC]}\n")
        os.replace(new_csv_path, csv_path)
