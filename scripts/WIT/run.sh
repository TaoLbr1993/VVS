#!/bin/bash

ORIGINAL_DIR=$(pwd)
cd ../../cpp/buildSQLite
broker_ip="localhost:50056"
ip_path="../../scripts/WIT/ip.txt"
filter_ip_path="../../scripts/WIT/filterIp.txt"
query_k=128
query_path="../../testdata/res/WIT/query.txt"
truth_path="../../testdata/res/WIT/gt_128_5.ivecs"
output_path="output_WIT.txt"
message_path="message_WIT.txt"

./user  --broker-ip=$broker_ip --silo-ip=$ip_path --query-k=$query_k --query-path=$query_path --output-path=$output_path --message-path=$message_path --truth-path=$truth_path --filter-ip=$filter_ip_path
if [ $? -ne 0 ]; then  
    echo "Query user FAIL"  
    exit 1  
fi 

cd "$ORIGINAL_DIR"