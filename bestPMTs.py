import pandas as pd
from show_table import *


class PMTs():
    def __init__(self, table_enpmts):
        # channels that are not mapped to any PMT were marked as HoleLabel 0
        self.table_enpmts = table_enpmts[table_enpmts['HoleLabel'] != 0]

    def best_N_livetime(self,table_lt,N):
        ''' obtain top N PMTs by livetime '''
        ## get livetime info
        self.table_enpmts['Livetime'] = self.table_enpmts.RunNumber.map(dict(zip(table_lt.index, table_lt['Livetime'])))

        ## get total livetime of each PMT
        # information of livetime of each PMT in each run
        # get rid of channel info (don't need anymore)
        self.table_enpmts = self.table_enpmts.drop('ChannelID', axis=1)
        # livetime of each pmt
        self.pmt_lt = self.table_enpmts.groupby('HoleLabel').sum()[['Livetime']]

        ## select top N
        self.pmt_lt = self.pmt_lt.sort_values('Livetime', ascending=False)
        self.pmt_lt = self.pmt_lt.iloc[:N]

        ## add relative livetime info
        # total livetime
        lt_tot = table_lt.sum()['Livetime']
        self.pmt_lt['LivetimeRelative'] = self.pmt_lt['Livetime'] / lt_tot
        print self.pmt_lt
#
#    ## save
#    outname = 'top{0}pmts_livetime.csv'.format(N)
#    print '-->', outname
#    pmt_lt.to_csv(outname, index=True)
#    return outname


def dark_noise_table():
    # dark noise info for each channel in each run
    # add hole label info
    pmt_info = map_to_hole.remap('DarkNoise.csv')
    # resulting info
    table_dn = pd.read_csv('DarkNoiseHoleLabel.csv')
    # average across all runs
    pmt_dn = table_dn.groupby('HoleLabel').mean()['DarkNoise']
    outname = 'DarkNoiseAverage.csv'
    pmt_dn.to_csv(outname, index=True)


def best_M_darknoise(pmt_info, M=900):
    ''' obtain top M PMTs by dark noise '''
    # average dark noise in all runs
    table_dn = pd.read_csv('DarkNoiseAverage.csv')
    table_dn = table_dn.set_index('HoleLabel')
    # PMTs to select from
    table_pmts = pd.read_csv(pmt_info)
    # add dark noise info
    table_pmts = table_pmts.set_index('HoleLabel', drop=False)
    table_pmts['DarkNoise'] = table_dn['DarkNoise'] # will match by index
    table_pmts = table_pmts.sort_values('DarkNoise')
    table_pmts = table_pmts.iloc[:M]
    # save final table
    outname = pmt_info.split('.') + 'top{0}_darknoise'.format(M)
    print '->', outname + '.csv'
    table_pmts.drop('HoleLabel', axis=1).to_csv(outname + '.csv', index=True)
    # save list of HoleLabels
    table_pmts['HoleLabel'].to_csv(outname + '.list', header=False, index=False)
    print '->', outname + '.list'
