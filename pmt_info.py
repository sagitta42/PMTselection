print 'Importing modules...'
import pandas as pd
import numpy as np
import pandas.io.sql as sqlio
import psycopg2
import os
from show_table import *
import sys
import map_pmts


class PMTinfo():
    def __init__(self, rmin, rmax):
        ''' rmin and rmax - range of runs in which the selection will be done.
            The specific runs themselves will be taken from the livetime table
        '''            
        ## livetime table
        # obtained from the DSTs
        # the runs in this table are the ones that will be analyzed for the PMT selection 
        print 'Reading livetime table...'
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
        print 'Given range: {0} - {1}'.format(rmin, rmax)
        print 'Resulting range: {0} - {1}'.format(self.run_min, self.run_max)

        self.table_lt = self.table_lt.set_index('RunNumber')
        show_table(self.table_lt)

        ## other input tables
        # disabled channels
        self.table_dis = pd.DataFrame()
        # reference channels
        self.table_ref = pd.DataFrame()

        ## tables obtained in the process
        # table with enabled channels in each run
        self.table_enchan_name = 'livetime/live_channels_run.csv'
        self.table_enchan = pd.DataFrame()
        # same table + PMT hole label info
        self.table_enpmt = pd.DataFrame()
        self.table_enpmt_name = 'livetime/live_channels_runHoleLabel.csv'
        # livetime of each PMT 
        self.table_best_pmts = pd.DataFrame()
        # average dark noise
        self.table_dark_avg_name = 'dark_noise/DarkNoiseAverage.csv'
        self.table_dark_avg = pd.DataFrame()


    def enabled_channels(self):
        ''' Obtain list of enabled channels for each run'''

        ## get tables with disabled and reference channels
        # will read saved files if exist, otherwise read from database (takes time)
        # if saved files do not cover run range, will update
        self.get_tables()
   
        ## all possible channel IDs
        allchans = range(1,2241) 
        # print out counter
        cnt = 0
        nruns = len(self.runs)
    
        # collect enabled ordinary channels and their livetime in each run
        print 'Collecting enabled channels in each run (will take a long while)...'
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
            # it seems that searching on a smaller table is faster, let's remove this run alltogether
            self.table_dis = self.table_dis.drop(r, axis=0)
            self.table_ref = self.table_ref.drop(r, axis=0)

        # save table            
        self.table_enchan.to_csv(self.table_enchan_name, index=False)
        print '-->', self.table_enchan_name


    def map_to_PMT(self):       
        # if the table table_enchan is empty (i.e. we skipped this step)
        # but the needed file does not exist, notify user
        if (not os.path.exists(self.table_enchan_name)) and len(self.table_enchan) == 0:
            print 'Table {0} does not exist. Do step 1: enabled channels in each run.'.format(self.table_enchan_name)
            sys.exit(1)

        # if empty table given, will read from storage
        self.table_enpmt = map_pmts.map_lg_to_pmt(self.table_enchan_name, self.table_enchan)


    def best_N_livetime(self, N):
        ''' obtain top N PMTs by livetime '''

        ## mapped PMTs table
        # if empty, means mapping was skipped -> read from storage
        if len(self.table_enpmt) == 0:
            # tell user if the table does not exist
            if not os.path.exists(self.table_enpmt_name):
                print 'Table {0} does not exist. Do step 2: map channels to PMT'.format(self.table_enpmt_name)
                sys.exit(1)
            print 'Reading table {0} from storage...'.format(self.table_enpmt_name)
            self.table_enpmt = pd.read_csv(self.table_enpmt_name)

        print 'Obtaining top {0} PMTs by livetime...'.format(N)
        # channels that are not mapped to any PMT were marked as HoleLabel 0 -> remove
        self.table_enpmt = self.table_enpmt[self.table_enpmt['HoleLabel'] != 0]
        ## get livetime info
        self.table_enpmt['Livetime'] = self.table_enpmt.RunNumber.map(dict(zip(self.table_lt.index, self.table_lt['Livetime'])))

        ## get total livetime of each PMT
        # get rid of channel info (don't need anymore)
        self.table_enpmt = self.table_enpmt.drop('ChannelID', axis=1)
        # livetime of each pmt
        self.table_best_pmts = self.table_enpmt.groupby('HoleLabel').sum()[['Livetime']]
        # add HoleLabel as column (needed later)
        self.table_best_pmts['HoleLabel'] = self.table_best_pmts.index

        ## select top N
        self.table_best_pmts = self.table_best_pmts.sort_values('Livetime', ascending=False)
        self.table_best_pmts = self.table_best_pmts.iloc[:N]

        ## add relative livetime info
        # total livetime
        lt_tot = self.table_lt.sum()['Livetime']
        self.table_best_pmts['LivetimeRelative'] = self.table_best_pmts['Livetime'] / lt_tot
        show_table(self.table_best_pmts)

        ## save
        best_pmts_name = 'livetime/top{0}pmts_livetime_runs{1}-{2}.csv'.format(N, self.run_min, self.run_max)
        self.table_best_pmts.to_csv(best_pmts_name, index=True)
        print '-->', best_pmts_name


    def avg_dark(self):
        ''' Obtain average dark noise in each PMT'''
        ## get dark noise info
        tname = "dark_noise/DarkNoise.csv"
        query = 'select * from "InnerPmtsDarkRate" where "RunNumber" <={0}  and "RunNumber" >= {1} order by "RunNumber", "ChannelID"'.format(self.run_max, self.run_min)
        print 'Obtaining average dark noise...'
        # will read from storage if already exists
        print '...getting dark noise info...'
        table_dark = self.read_table(tname, query, "bx_calib")
        # remove dead channel entires
        table_dark = table_dark[table_dark['DarkSigma'] > 0]
        # map to PMTs (will also save to file)
        print '...mapping channels to PMTs...'
        table_dark = map_pmts.map_lg_to_pmt(tname, table_dark)
        # remove HoleLabel 0 (channels that are not mapped to PMTs)
        table_dark = table_dark[table_dark['HoleLabel'] != 0]
        # calculate average
        print '...obtaining average dark noise.'
        self.table_dark_avg = table_dark.groupby('HoleLabel').mean()[['DarkNoise']]
        # save
        self.table_dark_avg.to_csv(self.table_dark_avg_name, index=True)
        print '-->', self.table_dark_avg_name



    def best_M_darknoise(self, N, M):       
        ''' Select top M PMTs by dark noise out of the previously obtained
            top N by livetime
        ''' 

        ## info about top N PMTs by livetime
        # if empty, means step was skipped
        best_pmts_name = 'livetime/top{0}pmts_livetime_runs{1}-{2}.csv'.format(N, self.run_min, self.run_max)
        if len(self.table_best_pmts) == 0:
            # tell user if the table does not exist
            if not os.path.exists(best_pmts_name):
                print 'Table {0} does not exist. Do step 3: select best PMTs by livetime'.format(best_pmts_name)
                sys.exit(1)
            print 'Reading table {0} from storage...'.format(best_pmts_name)
            self.table_best_pmts = pd.read_csv(best_pmts_name)


        ## info about avg dark noise
        # if empty, means avg dark rate calculation was skipped -> read from storage
        if len(self.table_dark_avg) == 0:
            # tell user if the table does not exist
            if not os.path.exists(self.table_dark_avg_name):
                print 'Table {0} does not exist. Do step 4: obtain avg dark noise in PMTs'.format(self.table_dark_avg_name)
                sys.exit(1)
            print 'Reading table {0} from storage...'.format(self.table_dark_avg_name)
            self.table_dark_avg = pd.read_csv(self.table_dark_avg_name)
            self.table_dark_avg = self.table_dark_avg.set_index('HoleLabel') # same format as when in the class 

        ## map dark noise info
        self.table_best_pmts['DarkNoise'] = self.table_best_pmts.HoleLabel.map(dict(zip(self.table_dark_avg.index, self.table_dark_avg['DarkNoise'])))

        ## sort by dark noise
        self.table_best_pmts = self.table_best_pmts.sort_values('DarkNoise')
        self.table_best_pmts = self.table_best_pmts.iloc[:M]
        outname = best_pmts_name.split('.')[0].split('/')[-1] + '_top{0}darknoise.csv'.format(M)
        # order the columns as wanted
        self.table_best_pmts = self.table_best_pmts[['HoleLabel', 'DarkNoise', 'LivetimeRelative', 'Livetime']]
        self.table_best_pmts.to_csv(outname, index=False)
        print '### FINAL ###'
        show_table(self.table_best_pmts)
        print '-->', outname
        print '#############'


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
        # if the table already exists, it will not be read again
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


    def read_table(self, tname, query, dbname):    
        ''' Read or create a table with given name.'''

        # if the table already exists, read it
        if os.path.exists(tname):
            print '...reading table {0} from storage'.format(tname)
            table = pd.read_csv(tname)
            if 'RunNumber' in table:
                trmin = table['RunNumber'].min()
                trmax = table['RunNumber'].max()
                print 'Run range: {0} - {1}'.format(trmin, trmax)
                # if our range of runs is wider, read again (improvement: only read the missing parts and concatenate)
                if (self.run_min < trmin or self.run_max > trmax):
                    table = self.table_query(tname, query, dbname)
                else:
                    # shorten to our range of runs not to have too much info
                    table = table[table['RunNumber'] >= self.run_min]
                    table = table[table['RunNumber'] <= self.run_max]
                    trmin = table['RunNumber'].min()
                    trmax = table['RunNumber'].max()
                    print '--> {0} - {1}'.format(trmin, trmax)
        # if the table doesn't exist, create it
        else:
            table = self.table_query(tname, query, dbname)
        return table


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
        print '-->', tname
        return dat





######################
## helper functions ##
######################


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

def show_table(table):
    print '----------------------------------'
    print table.head(10)
    print '...'
#    print table.tail(5)
    print '----------------------------------'
