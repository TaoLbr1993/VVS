#!/bin/bash

ORIGINAL_DIR=$(pwd)
cd ../../cpp/buildSQLite
filter_id=3
number=$((0*5 + ${filter_id}))
data_path="../../testdata/res/deep/deep_${number}.fivecs"
scalardata_path="../../testdata/res/deep/meta_${number}.txt"
table_name="filter_${number}"
sql_db_name="../../database/filter_Deep.db"
ipaddr=localhost:50060

./filter --ip=$ipaddr --sql-db-name=$sql_db_name --data-name=$data_path --scalar-name=$scalardata_path --table-name=$table_name

cd $ORIGINAL_DIR