#!/bin/bash

set -e  # fail and exit on any command erroring

if [ "$#" -ne 1 ]; then
  echo "Illegal number of parameters"
  echo "Usage: ./sample.sh (data directory)"
  exit
fi

root_dir=$(cd `dirname $0`/..; pwd)
build_dir=${root_dir}/build
data_dir=${1}
train_dir=${root_dir}/nf_train
exec=${build_dir}/sample
data_type='workload'

if [ ! -d ${train_dir} ];
then
  mkdir -p ${train_dir}
fi

# Configurations
declare -A key_type
key_type=([longitudes-200M]='double'
          [longlat-200M]='double'
          [ycsb-200M]='uint64'
          [books-200M]='uint64'
          [fb-200M]='uint64'
          [wiki-ts-200M]='uint64'
          [lognormal-200M]='uint64'
          [lognormal-190M]='int64')
sample_ratios=(0.05)
double_workloads=('longlat-200M' 'longitudes-200M')
int64_workloads=('lognormal-190M')
uint64_workloads=('ycsb-200M' 'fb-200M' 'lognormal-200M' 'books-200M' 'wiki-ts-200M')

for sample_ratio in ${sample_ratios[*]}
do
  for workload in ${uint64_workloads[*]}
  do
    for key_path in ${data_dir}/${workload}*.bin
    do
      echo 'Sample '`basename ${key_path}`
      if [ -f ${key_path} ];
      then
        echo `${exec} --key_path=${key_path} --data_type=${data_type} --key_type=${key_type[$workload]} --sample_ratio=${sample_ratio} --output_dir=${train_dir}`
      fi
    done
  done

  for workload in ${int64_workloads[*]}
  do
    for key_path in ${data_dir}/${workload}*.bin
    do
      echo 'Sample '`basename ${key_path}`
      if [ -f ${key_path} ];
      then
        echo `${exec} --key_path=${key_path} --data_type=${data_type} --key_type=${key_type[$workload]} --sample_ratio=${sample_ratio} --output_dir=${train_dir}`
      fi
    done
  done

  for workload in ${double_workloads[*]}
  do
    for key_path in ${data_dir}/${workload}*.bin
    do
      echo 'Sample '`basename ${key_path}`
      if [ -f ${key_path} ];
      then
        echo `${exec} --key_path=${key_path} --data_type=${data_type} --key_type=${key_type[$workload]} --sample_ratio=${sample_ratio} --output_dir=${train_dir}`
      fi
    done
  done
done