import pmt_info

###### -----------------------------------

## range of runs for the selection of the PMT set
run_min = 5007 # May 2007 Phase I start
run_max = 31095 # July 2019, moment of study
#run_max = 7000 # test

## top N PMTs by livetime
N = 1000
## top M PMTs by dark noise
M = 900

###### -----------------------------------

PMT_info = pmt_info.PMTinfo(run_min,run_max)

print '\n#######################################'
print 'Step 1: Enabled channels in each run'
print '#######################################\n'

## [!] comment out the following line if you want to skip this step (if already done)
PMT_info.enabled_channels()
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
print 'Step 4: select best PMTs by dark noise'
print '#######################################\n'
PMT_info.best_M_darknoise(N, M)


## after considering many types of selection, B900 were selected as
## top 900 PMTs by dark noise out of top 1000 by livetime (N=1000, M=900)
