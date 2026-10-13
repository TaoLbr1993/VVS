#!/bin/bash

ORIGINAL_DIR=$(pwd)
cd ../../cpp/buildSQLite
broker_ip="localhost:50056"
ip_path="../../scripts/DeepRun/ip.txt"
filter_ip_path="../../scripts/DeepRun/filterIp.txt"
query_k=128
query_path="../../testdata/res/deep/query.txt"
truth_path="../../testdata/res/deep/gt_128_5.ivecs"
output_path="output_Deep.txt"
message_path="message_Deep.txt"

./user  --broker-ip=$broker_ip --silo-ip=$ip_path --query-k=$query_k --query-path=$query_path --output-path=$output_path --message-path=$message_path --truth-path=$truth_path --filter-ip=$filter_ip_path
if [ $? -ne 0 ]; then  
    echo "Query user FAIL"  
    exit 1  
fi 

cd "$ORIGINAL_DIR"