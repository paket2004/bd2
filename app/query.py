#!/usr/bin/env python3

import sys
import math
from pyspark.sql import SparkSession
from pyspark.rdd import RDD
from pyspark import SparkConf
from cassandra.cluster import Cluster


def get_data_from_table(session, table_name):
    print("-------------")
    print(len(list(session.execute(f"SELECT * FROM {table_name}"))))
    return list(session.execute(f"SELECT * FROM {table_name}"))

def parse_query(query_text):
    parsed_query = []
    for term in query_text.split():
        stripped_term = term.strip()
        if stripped_term:
            parsed_query.append(stripped_term.lower())
    return parsed_query

def bm25(query_terms, doc_id, doc_len, avg_doc_len, term_freqs, doc_freqs, total_docs):
    k1 = 1.2
    b = 0.75
    score = 0.0
    for term in query_terms:
        if term in term_freqs and doc_id in term_freqs[term]:
            tf = term_freqs[term][doc_id]
            df = doc_freqs.get(term, 0)
            if df > 0:
                idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1.0)
                score += idf * ((tf * (k1 + 1)) / (tf + k1 * (1 - b + b * doc_len / avg_doc_len)))
    return score

query_text = " ".join(sys.argv[1:])

spark = SparkSession.builder \
    .appName("BM25 implementation") \
    .config("spark.cassandra.connection.host", "cassandra-server") \
    .getOrCreate()
sc = spark.sparkContext

query_terms = parse_query(query_text)

cluster = Cluster(['cassandra-server'])
session = cluster.connect('big_data_assignment')
print('I CONNECTED TO CASSANDRA')
document_main_content = {}
for row in get_data_from_table(session, 'document_main_content'):
    document_main_content[row.doc_id] = row.content
print(len(document_main_content))
try:
    doc_freqs = {}
    for row in get_data_from_table(session, 'doc_freq'):
        doc_freqs[row.term] = row.doc_freq


    doc_lens = {}
    for row in get_data_from_table(session, 'doc_len'):
        doc_lens[row.doc_id] = row.doc_len


    term_freqs = {}
    for term in query_terms:
        rows = session.execute(f"SELECT doc_id, term_freq FROM term_freq WHERE term = '{term}'")
        term_freqs[term] = {row.doc_id: row.term_freq for row in rows}


    avg_doc_len = sum(doc_lens.values()) / len(doc_lens) if len(doc_lens) > 0 else 0
    doc_ids_rdd = sc.parallelize(list(doc_lens.keys()))

    doc_scores = doc_ids_rdd.map(
        lambda doc_id: (
            doc_id, 
            bm25(
                query_terms, 
                doc_id, 
                doc_lens.get(doc_id, 0), 
                avg_doc_len, 
                term_freqs, 
                doc_freqs, 
                len(doc_lens)
            )
        )
    )
    print('-------------------------------------')
    print(doc_scores)
    top_docs = doc_scores.sortBy(lambda x: -x[1]).take(5)
    print(f"\nTop 5 results for '{query_text}'")
    print("------------------------------------------------")
    print(top_docs)
    for i, (doc_id) in enumerate(top_docs, 1):
        print(f"    Text: {document_main_content[doc_id]}")
        print()

finally:
    cluster.shutdown()
    spark.stop()

