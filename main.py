import re
import os
import time
from collections import defaultdict
from csvfile import CsvHandler
from constants import *
from utils import *
from chardet.universaldetector import UniversalDetector
import codecs

csv_handler = CsvHandler()
list_text_dir = os.listdir(TEXT_DIR)

defined_encodings = defaultdict(int)
total_enc_time = 0

log_line_file(FILE_WTD, '0')


file_counter = 0
for text_file_name in list_text_dir:
    start_file_proc = time.time()

    file_counter += 1
    doc_word_dict = defaultdict(int)
    text_file_path = os.path.join(TEXT_DIR, text_file_name)
    temp_text_file_path = os.path.join(TEMP_ENCODING_DIR, text_file_name)
    print(f"File {file_counter}")
    # --- START Encoding check
    detector = UniversalDetector()
    print("Checking file's encoding")
    enc_start = time.time()
    try:
        with open(text_file_path, "rb") as file:
            for line in file:
                detector.feed(line)
                if detector.done:
                    break
        detector.close()
        det_result = detector.result["encoding"]
        defined_encodings[det_result] += 1
        print(det_result)
        if det_result != ENCODING:
            wrong_encoding = det_result
            with codecs.open(text_file_path, "rU", wrong_encoding) as wr_enc_file:
                with codecs.open(temp_text_file_path, "w+", ENCODING) as cor_enc_file:
                    for line in wr_enc_file:
                        cor_enc_file.write(line)
            text_file_path = temp_text_file_path
    except:
        defined_encodings["Error"] += 1
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
    with open(text_file_path, "r", encoding=ENCODING) as text_file:
        for line in text_file:
            processed_line = sanitize_line(line)
            line_word_dict = get_words_dict(processed_line)
            for key in line_word_dict.keys():
                doc_word_dict[key] += line_word_dict[key]
        log_line_file(FILE_WFL, str(len(doc_word_dict.keys())))
        print(len(doc_word_dict.keys()))
        # pretty_print_dict(doc_word_dict)
    csv_handler.add_dict(CSV_FILE, doc_word_dict)

    with open(CSV_FILE, 'r', encoding='utf-8') as check_file:
        counter = 0
        for _ in check_file:
            counter += 1
        log_line_file(FILE_WTD, str(counter))

    end_file_proc = time.time() - start_file_proc
    log_line_file(FILE_TOT, str(end_file_proc))

# --- START Delete temp files
list_temp_text_dir = os.listdir(TEMP_ENCODING_DIR)
for temp_file_name in list_temp_text_dir:
    os.remove(os.path.join(TEMP_ENCODING_DIR, temp_file_name))
# --- END Delete temp files
print(defined_encodings)
print(f"Total encoding time {total_enc_time}")