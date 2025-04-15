#!/bin/bash
# Start ssh server
service ssh restart

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz
echo "Initializing cassandra"
until cqlsh cassandra-server -e "DESCRIBE KEYSPACES" > /dev/null 2>&1; do
  sleep 3
done
echo "Cassandra initialized"

cat > /tmp/initialize_cassandra_keyspace.cql << EOF
CREATE KEYSPACE IF NOT EXISTS big_data_assignment
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};


USE big_data_assignment;

CREATE TABLE IF NOT EXISTS vocabulary (
    term text PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS document_main_content (
    doc_id text PRIMARY KEY,
    content text
);

CREATE TABLE IF NOT EXISTS doc_freq (
    term text PRIMARY KEY,
    doc_freq int
);

CREATE TABLE IF NOT EXISTS doc_len (
    doc_id text PRIMARY KEY,
    doc_len int
);

CREATE TABLE IF NOT EXISTS term_freq (
    term text,
    doc_id text,
    term_freq int,
    PRIMARY KEY (term, doc_id)
);
EOF

# Initialize cassandra
cqlsh cassandra-server -f /tmp/initialize_cassandra_keyspace.cql


echo "Cassandra tables created succesfully"
# Collect data
bash prepare_data.sh


# Run the indexer
bash index.sh 

# Run the ranker
bash search.sh "How are you?"