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

total_enc_time = 0
log_line_file(FILE_WTD, '0')

file_counter = 0
for doc_name in docs_list:
    start_file_proc = time.time()
    file_counter += 1
    print(f"File {file_counter}")

    doc_path = os.path.join(TEXT_DIR, doc_name)

    # --- START Encoding check
    detector = UniversalDetector()
    print("Checking file's encoding")
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
        log_line_file(FILE_ENC, str(time.time() - enc_start))
        log_line_file(FILE_DUP, '-')
        log_line_file(FILE_WRI, '-')
        log_line_file(FILE_TOT, str(time.time() - start_file_proc))
        log_line_file(FILE_WFL, '-')
        log_line_file(FILE_WTD, '-')
        continue
    log_line_file(FILE_ENC, str(time.time() - enc_start))
    total_enc_time += time.time() - enc_start
    # --- END Encoding check

    words_in_doc = defaultdict(int)
    with open(doc_path, "r", encoding=ENCODING) as doc:
        for line in doc:
            sanitized_line = sanitize_line(line)
            words_in_line = get_words_dict(sanitized_line)
            for key in words_in_line.keys():
                words_in_doc[key] += words_in_line[key]
        log_line_file(FILE_WFL, str(len(words_in_doc.keys())))
    csv_handler.add_dict(CSV_FILE, words_in_doc)

    with open(CSV_FILE, 'r', encoding='utf-8') as csv_file:
        counter = 0
        for _ in csv_file:
            counter += 1
        log_line_file(FILE_WTD, str(counter))

    end_file_proc = time.time() - start_file_proc
    log_line_file(FILE_TOT, str(end_file_proc))

# --- START Delete temp files
for temp_enc_file in os.listdir(TEMP_ENCODING_DIR):
    os.remove(os.path.join(TEMP_ENCODING_DIR, temp_enc_file))
# --- END Delete temp files
