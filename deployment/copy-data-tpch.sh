#!/bin/sh

dbgen_dir=/tank/hdfs/ruiliu/rotary-aqp/tpch-dbgen
tpch_data_dir=/tank/hdfs/ruiliu/rotary-aqp/tpch-data
table_suffix=$1

for table in orders lineitem customer part partsupp supplier nation region
do
    if [ ! -d "$tpch_data_dir/${table_suffix}/${table}" ]
    then
        mkdir -p "$tpch_data_dir/${table_suffix}/${table}"
    fi

    cp $dbgen_dir/${table}.tbl $tpch_data_dir/${table_suffix}/${table}
done

