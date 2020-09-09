import pandas as pd
import numpy as np

def enabled_livetime(output_name='livetime_run_channels.csv'):
	## livetime table
	## the runs in this table define the range in which the PMTs will be analyzed
	print 'Livetime...'
	table_lt = pd.read_csv('livetime_run.csv')
	runs = table_lt['RunNumber']
	table_lt = table_lt.set_index('RunNumber')

	## disabled channels
	print 'Disabled channels...'
	table_dis = pd.read_csv('DisabledChannels.csv')
	# select only the portion of this table corresponding to runs in the livetime table
	table_dis = table_dis[table_dis['RunNumber'].isin(runs)]
	table_dis = table_dis.drop_duplicates() # some channels are present twice ("disabled twice")
	table_dis = table_dis.set_index('RunNumber')
	# the only information we need is run number and ID of disabled channels
	table_dis = table_dis['ChannelID']

	## reference channels in each run
	print 'Reference channels...'
	table_ref = pd.read_csv('LabenChannelMapping.csv')
	table_ref = table_ref.set_index('ProfileID')
	# leave channels that are NOT ordinary (laser ref, CNGS ref and any other)
	table_ref = table_ref[table_ref['ChannelDescription'] != 'Ordinary']
	# the only info we need is profile and ID of non-ordinary (reference) channels
	table_ref = table_ref['ChannelID']



	## create a table with active channels in each run and livetime info
	# future output file
	out = open(output_name,'w')
	print >> out, 'RunNumber', 'ChannelID', 'Livetime'
	allchans = range(1,2241) # all possible channel IDs
	print 'Obtaining enabled channels and livetime...'
	# print out counter
	cnt = 0
	nruns = len(runs)

	# collect enabled ordinary channels and their livetime in each run
	for r in runs:
		if cnt % 100 == 0: print cnt, '/', nruns, 'runs'
		# remove disabled channels in this run
		dischans = table_dis.loc[r]
		chans = np.setdiff1d(allchans,dischans)
		# remove reference channels in this profile
		p = profile(r)
		refchans = table_ref.loc[p]
		chans = np.setdiff1d(chans, refchans)
		# save
		for c in chans:
			print >> out, r, c, table_lt.loc[r]
		cnt += 1

	out.close()
	print 'Done'



def profile(r):
	if r >= 29090: return 25
	elif r >= 29059: return 24
	elif r >= 29010: return 23
	elif r >= 28928: return 22
	elif r >= 28884: return 21
	elif r >= 28740: return 20
	elif r >= 18000: return 19
	elif r >= 12001: return 18
	elif r >= 6413: return 17
	elif r >= 5542: return 16
	elif r >= 3231: return 15
	else: return 0 # in case empty initialization needed (right now not)
