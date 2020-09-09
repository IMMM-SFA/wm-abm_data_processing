import pandas as pd
pd.set_option('display.expand_frame_repr', False)

#cdl_lookup = pd.read_csv('//PNL/Users/YOON644/Projects/wm abm data/nldas cdl/cdl_gcam_lookup_v2.csv')
cdl_lookup = pd.read_csv('data/cdl_gcam_lookup_v3_notavailcorr.csv')

for year in range(2008,2018):
    name = 'cdl' + str(year) + '_clean.csv'
    data = pd.read_csv('//PNL/Users/YOON644/Projects/wm abm data/nldas cdl/' + name)
    data = pd.melt(data, id_vars=['NLDAS_ID'])
    data['variable'] = data['variable'].astype(int)
    data['year'] = year
    data = pd.merge(data, cdl_lookup, how='left',left_on='variable', right_on='CDL_id')
    if year == 2008:
        alldata = data
    else:
        alldata = alldata.append(data)

alldata['GCAM_name'] = alldata['GCAM_name'].fillna('NotAvailable') # fill nans (out of domain) with "NotAvailable" category

aggregation_functions = {'variable': 'first', 'value': 'sum', 'CDL_id': 'first', 'GCAM_id': 'first'}
alldata_new = alldata.groupby(['NLDAS_ID','GCAM_name','year'], as_index=False).aggregate(aggregation_functions)

nass_data = pd.read_csv('/Projects/wm abm data/nass database/qs.crops_20190123.txt', sep='\t')

pivot = pd.pivot_table(alldata, index = 'year',values='value',columns='GCAM_name',aggfunc=np.sum)

pivot_melt = pd.melt(pivot, 'year', var_name=)

d = pd.melt(df, 'Country Name', var_name='Date', value_name='GDPperCapGrowth%')