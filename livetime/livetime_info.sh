#!/bin/bash
# step 1: create list of DSTs from the given run range
python2 livetime/dst_list.py $1 $2
# creates file livetime_run.csv

# step 2: create file with livetime for each run
macro="livetime/livetime_dst.C"
g++ -Wall -O2 -o ${macro%.*} $macro `root-config --cflags --glibs`
./${macro%.*} $1 $2
