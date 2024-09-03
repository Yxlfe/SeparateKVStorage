#/bin/bash

exec="/home/zc/code/Wisckey_SeparateKVStorage/out-static/db_bench" 
# 获取脚本的绝对路径
script_path=$(readlink -f "$0")
script_dir_path=$(dirname "$script_path")
dbpath="/database_sdd/wisckeydb_dbbench_test"
benchmarks=fillrandom,stats
num=2097152
value_size=992
threads=1
histogram=1

log=db_bench.txt

if [ -f "$exec" ];then
    cp -f $exec $script_dir_path
else
    echo "exec ycsbc does not exist."
fi

echo 3 > /proc/sys/vm/drop_caches

date >> $log
cmd="./db_bench --db=$dbpath --benchmarks=$benchmarks --num=$num --value_size=$value_size --threads=$threads --histogram=$histogram >> $log"

echo "$cmd" >> $log
echo "$cmd"
eval "$cmd" 
if [ $? -ne 0 ]; then
    echo "Error: Command failed during load phase."
    exit 1
fi