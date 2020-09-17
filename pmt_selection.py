import pmt_info
import sys

###### -----------------------------------

## range of runs for the selection of the PMT set
#run_min = 5007 # May 2007 Phase I start
#run_max = 31095 # July 2019, moment of study
run_min = sys.argv[1]
run_max = sys.argv[2]

## top N PMTs by livetime
N = 1000
## top M PMTs by dark noise
M = 900

print ' ~~~ Selecting {0} PMTs by dark noise out of {1} by livetime'.format(M, N)

###### -----------------------------------

PMT_info = pmt_info.PMTinfo(int(run_min), int(run_max))

# the following steps can be commented out to save time in case the step was done
# be careful to comment them out in order
# you can comment out each previous step if you are sure it was already done/updated
# otherwise you might have clashes between updated and old info    

print '\n#######################################'
print 'Step 1: Enabled channels in each run'
print '#######################################\n'

## [!] comment out the following line if you want to skip this step (if already done)
PMT_info.enabled_channels()
# be careful about this step, if it's not up to date with your current run_min and run_max, the next steps will produce wrong results or possibly crash
## produces file livetime/live_channels_run.csv

print '\n#######################################'
print '## Step 2: Map channels to PMTs ##'
print '#######################################\n'

## [!] comment out the following line if you want to skip this step (if already done)
PMT_info.map_to_PMT()
## produces file livetime/live_channels_runHoleLabel.csv"

print '\n#######################################'
print 'Step 3: select best PMTs by livetime'
print '#######################################\n'

### step 3.1: select top N PMTs by livetime
## [!] comment out the following line if you want to skip this step (if already done)
PMT_info.best_N_livetime(N)
## produces file livetime/topNpmts_livetime.csv

print '\n#######################################'
print 'Step 4: obtain avg dark noise in PMTs'
print '#######################################\n'
## obtain average dark noise of all PMTs in given time range
# [!] comment out the following line if you want to skip this step (if already done)
PMT_info.avg_dark()   
## produces file dark_noise/DarkNoiseAverage.csv    

print '\n#######################################'
print 'Step 5: select best PMTs by dark noise'
print '#######################################\n'
PMT_info.best_M_darknoise(N, M)


## after considering many types of selection, B900 were selected as
## top 900 PMTs by dark noise out of top 1000 by livetime (N=1000, M=900)
