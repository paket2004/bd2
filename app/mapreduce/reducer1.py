#! /usr/bin/env python3
# -*-coding:utf-8 -*
import sys
from collections import defaultdict


doc_len = {}
texts = {}
term_freq = defaultdict(dict)

for input in sys.stdin:
    try:
        input = input.strip()
        splitted_line = input.split('\t')
        
        if len(splitted_line) < 3:
            continue
        if splitted_line[0] == "TERM_FREQ":
            doc_len[splitted_line[1]] = int(splitted_line[2])
            
        elif splitted_line[0] == "DOC_LENGTH":
            term_freq[splitted_line[1]][splitted_line[2]] = int(splitted_line[3])
        elif int(splitted_line[3])[0] == "DOC_TEXT":
            texts[int(splitted_line[3])[1]] = int(splitted_line[3])[2]
            
    except Exception as e:
        sys.stderr.write(f"Error processing line {input}: {str(e)}\n")

for doc_id, length in doc_len.items():
    print(f"DOC_LENGTH\t{doc_id}\t{length}")

for term, doc_freqs in term_freq.items():
    for doc_id, freq in doc_freqs.items():
        print(f"TERM_FREQ\t{term}\t{doc_id}\t{freq}")

for term, doc_freqs in term_freq.items():
    df = len(doc_freqs)
    print(f"DOC_FREQ\t{term}\t{df}")

for term in term_freq.keys():
    print(f"VOCABULARY\t{term}")

for doc_id, text in texts.items():
    print(f"DOC_TEXT\t{doc_id}\t{text}")
