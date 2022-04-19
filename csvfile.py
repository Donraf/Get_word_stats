import re
import os
from collections import defaultdict
from constants import *
from operator import itemgetter
import time
from utils import log_line_file


class CsvHandler:
    search_time = 0
    write_time = 0

    def check_for_duplicate(self, file, word_dict):
        words = []
        line_num = 0
        for line in file:
            line_num += 1
            word_csv = re.match(r'\w+', line).group()
            if word_csv in word_dict:
                words.append({
                    WORD: word_csv,
                    WORD_NUM_IN_DOC: word_dict[word_csv],
                    FIRST_OCCURRENCE: False,
                    WORD_NUM_LINE: line_num,
                })
                del word_dict[word_csv]
        for key in word_dict.keys():
            words.append({
                WORD: key,
                WORD_NUM_IN_DOC: word_dict[key],
                FIRST_OCCURRENCE: True,
                WORD_NUM_LINE: 0,
            })
        return words

    def add_dict(self, file_path, d: defaultdict):
        start_time = time.time()
        with open(file_path, "r", encoding=ENCODING) as file:
            words = self.check_for_duplicate(file, d)
        words = sorted(words, key=itemgetter(FIRST_OCCURRENCE, WORD_NUM_LINE, WORD_NUM_IN_DOC))
        end_search_time = time.time()
        log_line_file(FILE_DUP, str(end_search_time - start_time))
        print(f"Duplicate check time: {end_search_time - start_time}")
        self.search_time += end_search_time - start_time
        self.add_record(file_path, words)
        end_write_time = time.time()
        log_line_file(FILE_WRI, str(end_write_time - end_search_time))
        print(f"Write to a file time: {end_write_time - end_search_time}")
        self.write_time += end_write_time - end_search_time
        try:
            print(f"Total absolute time / Percentage:\n" +
                  f"Duplicate check: {self.search_time} / {self.search_time / (self.search_time + self.write_time) * 100}%" +
                  f"\nWrite to a file: {self.write_time} / {self.write_time / (self.search_time + self.write_time) * 100}%")
        except:
            pass

    def add_record(self, file_path, words):
        temp_file_path = os.path.join(TEMP_CSV_DIR, r"temp_csv.txt")
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

        with open(temp_file_path, "a+", encoding=ENCODING) as temp_file:
            with open(file_path, "r", encoding=ENCODING) as file:
                word_index = 0
                line_counter = 0
                for line in file:
                    line_counter += 1
                    if word_index != len(words) and \
                        not words[word_index][FIRST_OCCURRENCE] and \
                       words[word_index][WORD_NUM_LINE] == line_counter:
                        num_csv = re.search(r"\d+", line).group()
                        temp_file.write(f"{words[word_index][WORD]}" + CSV_DELIMITER +
                                        f"{words[word_index][WORD_NUM_IN_DOC] + int(num_csv)}\n")
                        word_index += 1
                    else:
                        temp_file.write(line)
                for index in range(word_index, len(words)):
                    temp_file.write(f"{words[index][WORD]}" + CSV_DELIMITER +
                                    f"{words[index][WORD_NUM_IN_DOC]}\n")
        os.replace(temp_file_path, file_path)
