#! /usr/bin/env python3
import sys
import os
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
nltk.download('stopwords', quiet=True)
nltk.download('punkt', quiet=True)
stop_words = set(stopwords.words('english'))
stemmer = PorterStemmer()

def preprocess_text(text):
    text = re.sub(r'[^a-zA-Z0-9\s]', ' ', text.lower())
    tokens = word_tokenize(text)
    processed_tokens = [stemmer.stem(token) for token in tokens if token.isalnum() and token not in stop_words]
    return processed_tokens

# mapper func
for line_num, line in enumerate(sys.stdin):
    try:
        parts = line.strip().split("\t")
        doc_id = parts[0]
        title = parts[1] 
        text = parts[2] 
        content = text.strip()
        doc_id = (str(doc_id) + "_" + title).replace(" ", "_")
        
        print(f"DOC_TEXT\t{doc_id}\t{content}")
        tokens = preprocess_text(content)
        doc_length = len(tokens)
        print(f"DOC_LENGTH\t{doc_id}\t{doc_length}")
        term_freq = {}
        for token in tokens:
            term_freq[token] = term_freq.get(token, 0) + 1
        
        for term, freq in term_freq.items():
            print(f"TERM_FREQ\t{term}\t{doc_id}\t{freq}")
            
    except Exception as e:
        sys.stderr.write(f"Error processing line {line_num}: {str(e)}\n")