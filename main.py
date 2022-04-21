import os
import time
import codecs
from collections import defaultdict

from chardet.universaldetector import UniversalDetector

from csvfile import CsvHandler
from constants import *
from utils import *


csv_handler = CsvHandler()
docs_list = os.listdir(TEXT_DIR)

log_line_file(FILE_WTD, '0')

file_counter = 0
while len(docs_list) > 0:
    start_file_proc = time.time()

    if len(docs_list) >= 100:
        docs_batch = docs_list[:BATCH_SIZE]
        docs_list = docs_list[BATCH_SIZE:]
    else:
        docs_batch = docs_list
        docs_list = []

    enc_time = 0
    dup_time = 0
    wri_time = 0
    words_in_dict = 0
    words_in_file = 0
    words_in_doc = defaultdict(int)

    for doc_name in docs_batch:

        file_counter += 1
        print(f"File {file_counter}")

        doc_path = os.path.join(TEXT_DIR, doc_name)

        detector = UniversalDetector()
        enc_start = time.time()
        try:
            with open(doc_path, "rb") as doc:
                for line in doc:
                    detector.feed(line)
                    if detector.done:
                        break
            detector.close()
            doc_encoding = detector.result["encoding"]
            print(doc_encoding)
            if doc_encoding != ENCODING:
                enc_doc_path = os.path.join(TEMP_ENCODING_DIR, doc_name)
                with codecs.open(doc_path, "rU", doc_encoding) as old_enc_doc, \
                        codecs.open(enc_doc_path, "w+", ENCODING) as new_enc_doc:
                    for line in old_enc_doc:
                        new_enc_doc.write(line)
                doc_path = enc_doc_path
        except:
            enc_time += time.time() - enc_start
            continue
        enc_time += time.time() - enc_start

        with open(doc_path, "r", encoding=ENCODING) as doc:
            for line in doc:
                sanitized_line = sanitize_line(line)
                words_in_line = get_words_dict(sanitized_line)
                for key in words_in_line.keys():
                    words_in_doc[key] += words_in_line[key]

    words_in_file = len(words_in_doc.keys())
    dup_time, wri_time = csv_handler.add_dict(CSV_FILE, words_in_doc)

    with open(CSV_FILE, 'r', encoding=ENCODING) as csv_file:
        for _ in csv_file:
            words_in_dict += 1

    for temp_enc_file in os.listdir(TEMP_ENCODING_DIR):
        os.remove(os.path.join(TEMP_ENCODING_DIR, temp_enc_file))

    tot_time = time.time() - start_file_proc

    log_line_file(FILE_ENC, str(enc_time))
    log_line_file(FILE_DUP, str(dup_time))
    log_line_file(FILE_WRI, str(wri_time))
    log_line_file(FILE_WFL, str(words_in_file))
    log_line_file(FILE_WTD, str(words_in_dict))
    log_line_file(FILE_TOT, str(tot_time))
