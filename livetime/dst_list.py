import os
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import sys

def dst_list(rmin, rmax):
    print '\n#######################################'
    print 'Step 0: Obtaining livetime from dsts'
    print '#######################################\n'

    # connect to database
    conn = psycopg2.connect("host='bxdb.lngs.infn.it' dbname='bx_runvalidation' user=borex_guest")
    # get ValidRuns table for given runs
    query = "select \"RunNumber\", \"Groups\", \"RootFiles\" from \"ValidRuns\" where \"RunNumber\" = {0} or \"RunNumber\" = {1} order by \"RunNumber\"".format(rmin, rmax)
    table = sqlio.read_sql_query(query, conn)
    conn.close()

    ## get the DST corresponding to the first and last run
    table['Year'] =  table['RootFiles'].str.split('cycle', expand=True)[1].str.split('/', expand=True)[1]
    table['DST'] = table['Year'] + '_' + table['Groups']

    ## get list of dsts between these two
    # years are different folders
    years = table['Year'].astype(int).unique()
    storage = '/storage/gpfs_data/borexino/dst/cycle_19/'
    # get dsts from all years in between
    years = range(min(years), max(years)+1)
    dst_years = []
    for y in years: dst_years += os.listdir(storage + str(y))
    # remove the ones before and after our range
    dst_years = pd.DataFrame({'path': dst_years})
    dst_years['dst'] = dst_years['path'].str.split('dst_', expand=True)[1].str.split('_c19', expand=True)[0]
    dst_years['year'] = dst_years['dst'].str.split('_', expand=True)[0]
    # to sort chronologically
    dst_years['date'] = pd.to_datetime(dst_years['dst'].str.replace('_', '-'))
    dst_years = dst_years.sort_values('date')
    dst_years = dst_years.reset_index() # purely for display
    print dst_years['dst'].iloc[:5].to_string()
    print '...'
    print dst_years['dst'].iloc[-5:-1].to_string()
    dst_years = dst_years.set_index('dst')
    dst_years = dst_years.loc[table['DST'].loc[0] : table['DST'].loc[1]]
    dst_years['path'] = storage + dst_years['year'] + '/' +  dst_years['path']

    # save
    outname = 'livetime/dst_list{0}-{1}.list'.format(rmin, rmax)
    dst_years['path'].to_csv(outname, index=False, header=False)
    print '-->', outname




if __name__ == '__main__':
#    dst_list(5007, 8001) test
    dst_list(*sys.argv[1:])
