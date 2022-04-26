import os
import time
import codecs
from collections import defaultdict
from multiprocessing import Process, Manager

from chardet.universaldetector import UniversalDetector

from csvfile import CsvHandler
from constants import *
from utils import *


def f(dict_queue, docs_batch):
    words_in_doc = defaultdict(int)

    for doc_name in docs_batch:
        doc_path = os.path.join(TEXT_DIR, doc_name)

        if CHECK_ENCODING:
            enc_start = time.time()
            detector = UniversalDetector()
            try:
                with open(doc_path, "rb") as doc:
                    for line in doc:
                        detector.feed(line)
                        if detector.done:
                            break
                detector.close()
                doc_encoding = detector.result["encoding"]
                if doc_encoding != ENCODING:
                    enc_doc_path = os.path.join(TEMP_ENCODING_DIR, doc_name)
                    with codecs.open(doc_path, "rU", doc_encoding) as old_enc_doc, \
                            codecs.open(enc_doc_path, "w+", ENCODING) as new_enc_doc:
                        for line in old_enc_doc:
                            new_enc_doc.write(line)
                    doc_path = enc_doc_path
            except:
                # enc_time += time.time() - enc_start
                continue
            # enc_time += time.time() - enc_start

        with open(doc_path, "r", encoding=ENCODING) as doc:
            for line in doc:
                sanitized_line = sanitize_line(line)
                words_in_line = get_words_dict(sanitized_line)
                for key in words_in_line.keys():
                    words_in_doc[key] += words_in_line[key]
    dict_queue.append(words_in_doc)

    # words_in_file = len(words_in_doc.keys())
    # dup_time, wri_time = csv_handler.add_dict(CSV_FILE, words_in_doc)

    # with open(CSV_FILE, 'r', encoding=ENCODING) as csv_file:
    #     for _ in csv_file:
    #         words_in_dict += 1

    # for temp_enc_file in os.listdir(TEMP_ENCODING_DIR):
    #     os.remove(os.path.join(TEMP_ENCODING_DIR, temp_enc_file))

    # tot_time = time.time() - start_file_proc

    # log_line_file(FILE_ENC, str(enc_time))
    # log_line_file(FILE_DUP, str(dup_time))
    # log_line_file(FILE_WRI, str(wri_time))
    # log_line_file(FILE_WFL, str(words_in_file))
    # log_line_file(FILE_WTD, str(words_in_dict))
    # log_line_file(FILE_TOT, str(tot_time))


if __name__ == '__main__':
    times = time.time()
    with open(FILE_ENC, 'w+', encoding=ENCODING) as file:
        file.write('ENCODING_TIME\n')
    with open(FILE_DUP, 'w+', encoding=ENCODING) as file:
        file.write('DUPLICATE_TIME\n')
    with open(FILE_WRI, 'w+', encoding=ENCODING) as file:
        file.write('WRITE_TIME\n')
    with open(FILE_WFL, 'w+', encoding=ENCODING) as file:
        file.write('WORDS_NUM_IN_FILE\n')
    with open(FILE_WTD, 'w+', encoding=ENCODING) as file:
        file.write('WORDS_NUM_IN_DICT\n')
    with open(FILE_TOT, 'w+', encoding=ENCODING) as file:
        file.write('TOTAL_TIME\n')
    with open(CSV_FILE, 'w+', encoding=ENCODING) as file:
        pass

    manager = Manager()
    dict_queue = manager.list()
    csv_handler = CsvHandler()
    docs_list = os.listdir(TEXT_DIR)

    file_counter = 0

    num_processes = 3
    batch_1 = []
    batch_2 = []
    batch_3 = []
    while len(docs_list) > 0:
        if len(docs_list) >= BATCH_SIZE * 3:
            batch_1 = docs_list[:BATCH_SIZE]
            batch_2 = docs_list[BATCH_SIZE:BATCH_SIZE * 2]
            batch_3 = docs_list[BATCH_SIZE * 2:BATCH_SIZE * 3]
            docs_list = docs_list[BATCH_SIZE * 3:]
        else:
            break

        p1 = Process(target=f, args=(dict_queue, batch_1))
        p1.start()
        p2 = Process(target=f, args=(dict_queue, batch_2))
        p3 = Process(target=f, args=(dict_queue, batch_3))
        p1.start()
        p2.start()
        p3.start()
        p1.join()
        p2.join()
        p3.join()

        print(len(dict_queue))
    print(time.time() - times)
