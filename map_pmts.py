import sys
import pandas as pd
import os

def map_lg_to_pmt(name, df=pd.DataFrame()):
    # output name
    outname = '.'.join(x for x in name.split('.')[:-1]) + 'HoleLabel.csv'

    ### table we want to remap

    if len(df) == 0:
        # if the given table is empty, read and map
        if not os.path.exists(name):
            print 'Table {0} does not exist. Please create it first.'
            sys.exit()
        print 'Reading {0} from storage...'.format(name)
        df = pd.read_csv(name)
    else:
        # get table if given
        df = df

    # only interested in profile 15 (>5000)
    df = df[df['RunNumber'] > 5000]
    df = df.sort_values(['RunNumber', 'ChannelID'])
    # runs = df['RunNumber'].unique()

    ### profile starts
    print 'Reading profile info...'
    dfp = pd.read_csv('hole_mapping/ProfileStartRun.csv')
    dfp = dfp.set_index('ProfileID')
    dfp = dfp['RunNumber']
    # add ProfileID info to our table
    print 'Adding profile info...'
    df = df.set_index('RunNumber', drop=False)
    df['ProfileID'] = -1
    # profiles 15 to 24
    for p in range(15,25):
        print p
        df.at[dfp.loc[p] : dfp.loc[p+1]-1, 'ProfileID'] = p
    # profile 25
    df.at[dfp.loc[25]:, 'ProfileID'] = 25
    print '25'

    ### hole labels
    print 'Mapping hole labels...'
    df['HoleLabel'] = -1 # do this to later check if everything is OK
    dfh = pd.read_csv('hole_mapping/HolesMapping.csv')
    dfh = dfh.sort_values(['ProfileID','ChannelID'])
    dfh = dfh.set_index(['ProfileID','ChannelID'])
    # df = df.sort_values(['ProfileID','ChannelID'])
    profiles = df['ProfileID'].unique()
    df = df.set_index(['ProfileID'])


    for p in profiles:
        print p
        # channels in our table in a given profile
        channels = list(df.loc[p]['ChannelID'].astype(int).unique())
        # corresponding PMTs
        pmts = dfh.loc[(p,channels),'HoleLabel']
        # add missing channels
        pmts.index = pmts.index.droplevel(0) # leave only channels as index, not profile
        pmts = pmts.reindex(channels)
        pmts = pmts.fillna(0) # if some channel is not there, means it disappeared, let's map it to hole label 0
        pmts = list(pmts)
        # add final hole label info to our table
        df.at[p, 'HoleLabel'] = df.loc[p].ChannelID.map(dict(zip(channels, pmts)))
#        df.at[p, 'HoleLabel'] = dfh.loc[(p,channels)]['HoleLabel'] # yypigonna insert NaN

    ### save
    print 'Saving...'
    df.to_csv(outname)
    print '-->', outname
    return df
#    return outname


if __name__ == '__main__':
    tbname = sys.argv[1]
    map_lg_to_pmt(tbname)
