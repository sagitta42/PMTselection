import pandas as pd
import map_to_hole

def best_N_livetime(pmt_info='livetime_run_channelsHoleLabel.csv', N=1000):
    ''' obtain top N PMTs by livetime '''

    ## get total livetime of each PMT
    # information of livetime of each PMT in each run
    table = pd.read_csv(pmt_info)
    # get rid of channel info (don't need anymore)
    table = table.drop('ChannelID', axis=1)
    pmt_lt = table.groupby('HoleLabel').sum()

    ## select top N
    pmt_lt = pmt_lt.sort_values('Livetime', ascending=False)
    pmt_lt = pmt_lt.iloc[:N]

    ## add relative livetime info
    # total livetime
    table_lt = pd.read_csv('livetime_run.csv')
    lt_tot = table_lt.sum()['Livetime']
    pmt_lt['LivetimeRelative'] = pmt_lt['Livetime'] / lt_tot

    ## save
    outname = 'top{0}pmts_livetime.csv'.format(N)
    print '-->', outname
    pmt_lt.to_csv(outname, index=True)
    return outname


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
