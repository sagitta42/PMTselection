#!/bin/bash
## Step 0: obtain livetime of given run range
# [!] comment out this line if this step is already done
./livetime/livetime_info.sh $1 $2

# PMT selection (for separte steps see python script)
python2 pmt_selection.py $1 $2


