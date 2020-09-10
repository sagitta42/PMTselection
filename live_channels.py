print 'Importing modules...'
import pandas as pd
import numpy as np
import pandas.io.sql as sqlio
import psycopg2
import os
from show_table import *
import sys


class LiveChannels():
    def __init__(self, rmin, rmax):
        ''' rmin and rmax - range of runs in which the selection will be done.
            The specific runs themselves will be taken from the livetime table
        '''            
        ## livetime table
        # obtained from the DSTs
        # the runs in this table are the ones that will be analyzed for the PMT selection 
        print 'Livetime...'
        ltname =  'livetime/livetime_run.csv'
        self.table_lt = pd.read_csv(ltname)
        self.table_lt = self.table_lt.sort_values('RunNumber')

        # determine min and max runs
        runs = self.table_lt['RunNumber']
        run_min = rmin if rmin else runs.min()
        run_max = rmax if rmax else runs.max()

        if run_min < runs.min():
            print 'Given rmin ({0}) out of bounds in {1} (min {2})'.format(rmin, ltname, runs.min())
            sys.exit(1)
        if run_max > runs.max():
            print 'Given rmax ({0}) out of bounds in {1} (max {2})'.format(rmax, ltname, runs.max())
            sys.exit(1)

        self.table_lt = self.table_lt[self.table_lt['RunNumber'] >= run_min]
        self.table_lt = self.table_lt[self.table_lt['RunNumber'] <= run_max]
        self.runs = self.table_lt['RunNumber']
        self.run_min = self.runs.min()
        self.run_max = self.runs.max()
        print 'Rmin {0} Rmax {1}'.format(self.run_min, self.run_max)

        self.table_lt = self.table_lt.set_index('RunNumber')
        show_table(self.table_lt)

        ## future output
        # table with enabled channels in each run
        self.table_enchan = pd.DataFrame()
        # same table + PMT hole label info
        self.table_enpmt = pd.DataFrame()


    def get_tables(self):
        ''' Get the DisabledChannel and LabenMapping tables.
            The info from these tables is used to remove disabled and non-ordinary channels
            to obtain enabled channels
        '''

        ## disabled channels
        print 'Disabled channels...'
        tname = "livetime/DisabledChannels.csv"
        query = 'select "RunNumber", "ChannelID" from "DisabledChannels" where "Cycle" = 18 and "RunNumber" >= {0} and "RunNumber" <= {1} and "Timing" = 1 order by "RunNumber", "ChannelID"'.format(self.run_min, self.run_max)
        dbname = 'bx_calib'
        self.table_dis = self.read_table(tname, query, dbname)

        self.table_dis = self.table_dis.drop_duplicates() # some channels are present twice ("disabled twice")
        self.table_dis = self.table_dis.set_index('RunNumber')
        # the only information we need is run number and ID of disabled channels
        show_table(self.table_dis)
        self.table_dis = self.table_dis['ChannelID']
    
        ## reference channels in each run
        print 'Reference channels...'
        tname = "livetime/LabenChannelMapping.csv"
        # only interested in runs > 5007 => profile from 15
        query = 'select "ProfileID", "ChannelID", "ChannelDescription" from "LabenChannelMapping" where "ProfileID" > 14 order by "ProfileID", "ChannelID"'
        dbname = 'daq_config'
        self.table_ref = self.read_table(tname, query, dbname)

        self.table_ref = self.table_ref.sort_values(['ProfileID', 'ChannelID'])
        self.table_ref = self.table_ref.set_index('ProfileID')
        # leave channels that are NOT ordinary (laser ref, CNGS ref and any other)
        self.table_ref = self.table_ref[self.table_ref['ChannelDescription'] != 'Ordinary']
        show_table(self.table_ref)
        # the only info we need is profile and ID of non-ordinary (reference) channels
        self.table_ref = self.table_ref['ChannelID']



    def enabled_channels(self):
        ''' Obtain list of enabled channels for each run'''
   
        ## all possible channel IDs
        allchans = range(1,2241) 
        # print out counter
        cnt = 0
        nruns = len(self.runs)
    
        # collect enabled ordinary channels and their livetime in each run
        print 'Collecting enabled channels in each run (may take a while)...'
        for r in self.runs:
            if cnt % 50 == 0: print cnt, '/', nruns, 'runs'
            # remove disabled channels in this run
            dischans = self.table_dis.loc[r]
            chans = np.setdiff1d(allchans,dischans)
            # remove reference channels in this profile
            p = profile(r)
            refchans = self.table_ref.loc[p]
            chans = np.setdiff1d(chans, refchans)
            # temporary table for this run
            df = pd.DataFrame({'ChannelID':chans})
            df['RunNumber'] = r
            # append temp. table to total table
            self.table_enchan = pd.concat([self.table_enchan, df], ignore_index=True)
            cnt += 1

        # save table            
        output_name = 'livetime/live_channels_run.csv'
        self.table_enchan.to_csv(output_name, index=False)
        print '-->', output_name
        print '##### Done #####'


    def table_query(self, tname, query, dbname):
        ''' Get table from database '''
        print '...reading from the database (may take a while)...'
        # connect to database
        conn = psycopg2.connect("host='bxdb.lngs.infn.it' dbname='{0}' user=borex_guest".format(dbname))
        # read table with given query
        dat = sqlio.read_sql_query(query, conn)
        conn.close()
        print '...saving.'
        dat.to_csv(tname, index=False)
        return dat


    def read_table(self, tname, query, dbname):    
        ''' Read or create a table with given name.'''

        # if the table already exists, read it
        if os.path.exists(tname):
            print '...reading the table'
            table = pd.read_csv(tname)
            # if our range of runs is wider, read again (improvement: only read the missing parts and concatenate)
            if 'RunNumber' in table and (self.run_min < table['RunNumber'].min() or self.run_max > table['RunNumber'].max()):
                table = self.table_query(tname, query, dbname)
        # if the table doesn't exist, create it
        else:
            table = self.table_query(tname, query, dbname)
        return table


# improvement: use ProfileRun table from hole mapping?
def profile(r):
    ''' Helper function to determine the profile of a run '''
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


if __name__ == '__main__':
    lch = LiveChannels()
    lch.get_tables()
    lch.enabled_channels()
