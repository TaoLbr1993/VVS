#!/bin/bash

ORIGINAL_DIR=$(pwd)
cd ../../cpp/buildSQLite
silo_id=1
exact_knn_option=2
number=$((0*5 + ${silo_id}))
data_path="../../testdata/res/deep/deep_${number}.fivecs"
scalardata_path="../../testdata/res/deep/meta_${number}.txt"
cluster_path="../../testdata/res/deep/deep_${number}.cluster/cluster"
collection_name="LOCAL_DATA_${number}"
milvus_port="19530"
cluster_option="OFF"
milvus_option="OFF"
alpha=0.05
cluster_num=10
ipaddr=localhost:50051
filter_ipaddr=localhost:50058

./silo --ip=$ipaddr --filter-ip=$filter_ipaddr --id=$number --exact-knn-option=$exact_knn_option --data-path=$data_path --scalardata-path=$scalardata_path --cluster-path=$cluster_path --index-type=HNSW --collection-name=$collection_name --milvus-port=$milvus_port --cluster-option=$cluster_option --milvus-option=$milvus_option --alpha=$alpha --cluster-num=$cluster_num

cd "$ORIGINAL_DIR"