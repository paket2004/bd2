#! /usr/bin/env python3
# -*-coding:utf-8 -*
import sys
import time
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel

try:
    # Connect to Cassandra and keyspace
    cluster = Cluster(['cassandra-server'])
    session = cluster.connect()
    session.execute("USE big_data_assignment")
    print("HEY MAN I AM HERE")
    # Prepare insert statements
    insert_vocabulary = session.prepare("INSERT INTO vocabulary (term) VALUES (?)")
    insert_doc_len = session.prepare("INSERT INTO doc_len (doc_id, doc_len) VALUES (?, ?)")
    insert_term_freq = session.prepare("INSERT INTO term_freq (term, doc_id, term_freq) VALUES (?, ?, ?)")
    insert_doc_freq = session.prepare("INSERT INTO doc_freq (term, doc_freq) VALUES (?, ?)")
    insert_text = session.prepare("INSERT INTO document_main_content (doc_id, content) VALUES (?, ?)")

    # else it doesn't work
    batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
    batch_size = 0
    max_batch_size = 1
    
   # in from reducer1.py
    for input in sys.stdin:
        try:
            input = input.strip()
            splitted_input = input.split('\t')
            if len(splitted_input) < 2:
                continue
            if splitted_input[0] == "TERM_FREQ":
                batch.add(insert_doc_len, (splitted_input[1], int(splitted_input[2])))
                batch_size += 1
            elif splitted_input[0] == "DOC_LENGTH":
                batch.add(insert_term_freq, (splitted_input[1], splitted_input[2], int(splitted_input[3])))
                batch_size += 1
            elif splitted_input[0] == "VOCABULARY":
                batch.add(insert_vocabulary, (splitted_input[1],))
                batch_size += 1
            elif splitted_input[0] == "DOC_FREQ":
                batch.add(insert_doc_freq, (splitted_input[1], int(splitted_input[2])))
                batch_size += 1
            elif splitted_input[0] == "DOC_TEXT":
                batch.add(insert_text, (splitted_input[1], splitted_input[2]))
                batch_size += 1
            #pass batch and reset batch_size
            if batch_size >= max_batch_size:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.ONE)
                batch_size = 0
                
        except Exception as e:
            sys.stderr.write("Error processing line")
    # sent remaining tasks
    if batch_size > 0:
        session.execute(batch)
    
except Exception as e:
    sys.stderr.write("error in reducer2\n")
    sys.exit(1)
    
finally:
    if 'cluster' in locals():
        cluster.shutdown()