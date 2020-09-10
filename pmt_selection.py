import live_channels 
import map_to_hole
import bestPMTs

## range of runs for the selection of the PMT set
run_min = 5007 # May 2007 Phase I start
#run_max = 31095 # July 2019, moment of study
run_max = 7000 # test

print '---------------------------------------'
print 'Step 1: Enabled channels in each run'
print '---------------------------------------'
## step 1: obtain enabled channels in each run
lch = live_channels.LiveChannels(run_min,run_max)
## comment out the following two lines if you want to skip this step (if channels already obtained)
#lch.get_tables()
#lch.enabled_channels()

print '---------------------------------------'
print 'Step 2: Map channels to PMTs'
print '---------------------------------------'
## step 2: map channels to hole labels of corresponding PMTs in each profile
## comment out this line if you want to skip this step (if already mapped before)
#lch.table_enpmt = map_to_hole.remap("livetime/live_channels_run.csv", lch.table_enchan if len(lch.table_enchan) else None)

#print '---------------------------------------'
#print 'Step 3: select best PMTs'
#print '---------------------------------------'
### step 3: select best PMTs. Current procedure:
### step 3.1: select top N PMTs by livetime
pmts = bestPMTs.PMTs(lch.table_enpmt if len(lch.table_enpmt) else live_channels.pd.read_csv('livetime/live_channels_runHoleLabel.csv'))
pmts.best_N_livetime(lch.table_lt, 1000)
### step 3.2: obtain average dark noise of all PMTs
#bestPMTs.dark_noise_table()
### step 3.3: select top M by dark noise
#bestPMTs.best_M_darknoise(topN)
## 2) add dark noise info and select top M PMTs by dark noise
## after considering many types of selection, B900 were selected as
## top 900 PMTs by dark noise out of top 1000 by livetime
