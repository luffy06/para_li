#!/bin/bash

set -e  # fail and exit on any command erroring

if [ "$#" -ne 1 ]; then
  echo "Illegal number of parameters"
  echo "Usage: ./sample.sh (keyset directory)"
  exit
fi

root_dir=$(cd `dirname $0`/..; pwd)
build_dir=${root_dir}/build
keyset_dir=${1}
train_dir=${root_dir}/nf_train
exec=${build_dir}/sample

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
    echo 'Sample '${workload}
    key_path=${keyset_dir}/${workload}.bin
    if [ -f ${key_path} ];
    then
      echo `${exec} --key_path=${key_path} --key_type=${key_type[$workload]} --sample_ratio=${sample_ratio} --output_dir=${train_dir}`
    fi
  done

  for workload in ${int64_workloads[*]}
  do
    echo 'Sample '${workload}
    key_path=${keyset_dir}/${workload}.bin
    if [ -f ${key_path} ];
    then
      echo `${exec} --key_path=${key_path} --key_type=${key_type[$workload]} --sample_ratio=${sample_ratio} --output_dir=${train_dir}`
    fi
  done

  for workload in ${double_workloads[*]}
  do
    echo 'Sample '${workload}
    key_path=${keyset_dir}/${workload}.bin
    if [ -f ${key_path} ];
    then
      echo `${exec} --key_path=${key_path} --key_type=${key_type[$workload]} --sample_ratio=${sample_ratio} --output_dir=${train_dir}`
    fi
  done
done