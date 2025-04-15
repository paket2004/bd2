#!/bin/bash

# Default input path
INPUT_PATH="/index/data"

# Check if an argument is provided
if [ "$#" -eq 1 ]; then
    INPUT_PATH="$1"
    
    # Check if the input is a local file, and if so, upload it to HDFS
    if [ -f "$INPUT_PATH" ]; then
        # Create a temporary directory in HDFS
        hdfs dfs -mkdir -p /tmp/index_input
        # Upload the file
        hdfs dfs -put "$INPUT_PATH" /tmp/index_input/
        # Update the input path to the HDFS path
        INPUT_PATH="/tmp/index_input/$(basename "$INPUT_PATH")"
    fi
fi

# Create temporary directories for intermediate output
hdfs dfs -rm -r /tmp/index/output1
hdfs dfs -rm -r /tmp/index/output2
# hdfs dfs -mkdir -p /tmp/index/output1
# hdfs dfs -mkdir -p /tmp/index/output2

echo "Starting indexing process for: $INPUT_PATH"
chmod +x $(pwd)/mapreduce/mapper1.py
chmod +x $(pwd)/mapreduce/reducer1.py
chmod +x $(pwd)/mapreduce/mapper2.py 
chmod +x $(pwd)/mapreduce/reducer2.py
# List all files in the input directory recursively and pass to first MapReduce job
echo "Running first MapReduce job..."
# hdfs dfs -find "$INPUT_PATH" -name "*.txt" | 

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz#.venv \
  -D mapreduce.reduce.memory.mb=4096 \
  -D mapreduce.reduce.java.opts=-Xmx1800m \
  -mapper ".venv/bin/python3 mapper1.py" \
  -reducer ".venv/bin/python3 reducer1.py" \
  -input "$INPUT_PATH" \
  -output "$TMP_OUT1" \

# check if the first job was successful
if [ $? -ne 0 ]; then
    echo "Error: First MapReduce job failed"
    exit 1
fi

echo "Running second MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -archives /app/.venv.tar.gz#.venv \
    -D mapreduce.reduce.memory.mb=4096 \
    -D mapreduce.reduce.java.opts=-Xmx1800m \
    -mapper ".venv/bin/python3 mapper2.py" \
    -reducer ".venv/bin/python3 reducer2.py" \
    -input "/tmp/index/output1" \
    -output "/tmp/index/output2" \


echo "Indexing completed successfully!"
echo "Index data has been stored in Cassandra database"

# Clean up temporary directories if needed
hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2