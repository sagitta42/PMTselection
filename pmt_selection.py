import channel_livetime
import map_to_hole
import bestPMTs

## output name of the table with livetime info for each enabled channel in each run
livetime_info = 'livetime_run_channels.csv'

print '---------------------------------------'
print 'Step 1: livetime of enabled channels'
print '---------------------------------------'
## step 1: obtain enabled channels in each run and livetime info
# input: livetime_run.csv, DisabledChannels.csv, LabenChannelMapping.csv
# output: given name (default livetime_run_channels.csv)
channel_livetime.enabled_livetime(livetime_info)

print '---------------------------------------'
print 'Step 2: livetime of enabled channels'
print '---------------------------------------'
## step 2: map channels to hole labels of corresponding PMTs in each profile
# input: ProfileStartRun.csv, HolesMapping.csv, given table
# output: table_nameHoleLabel.csv
# the function returns the name of the resulting table
pmt_info = map_to_hole.remap(livetime_info)


print '---------------------------------------'
print 'Step 3: select best PMTs'
print '---------------------------------------'
## step 3: select best PMTs. Current procedure:
# 1) select top N PMTs by livetime
# 2) add dark noise info and select top M PMTs by dark noise
# after considering many types of selection, B900 were selected as
# top 900 PMTs by dark noise out of top 1000 by livetime

## step 3.1
# input: livetime_run.csv, given table (default 'livetime_run_channelsHoleLabel.csv')
# returns the name of the resulting table
topN = bestPMTs.best_N_livetime(pmt_info, 1000)

## step 3.2: obtain average dark noise of all PMTs (can be done once)
bestPMTs.dark_noise_table()

## step 3.3: select top M by dark noise
bestPMTs.best_M_darknoise(topN)
