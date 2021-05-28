# Author: Jim Yoon, Pacific Northwest National Laboratory
# E-mail: jim.yoon@pnnl.gov

# The following script is used to estimate observed crop areas, irrigated vs non-irrigated areas,
# crop prices and costs, and other data at 1/8 degree resolution (following the NLDAS grid). The resuting
# data table serves as an input to the the parameterization of PMP agents for integration into MOSART-WM-ABM

#### Step 1 - Import Modules

import pandas as pd
import numpy as np
import math
pd.set_option('display.expand_frame_repr', False)  # Modifies pandas settings to display all columns of dataframes

#### Step 2 - Load External Data Tables

# Load CDL observed crop data as a pandas dataframe. CDL data has been aggregated to 1/8 degree resolution and assigned
# to GCAM crop categories as a pre-processing step in GIS.
cdl = pd.read_csv('../data/all_nldas_cdl_data_v3.txt')

#cdl_states = pd.read_csv('cdl_regions_join.csv')

# Load USDA Farm Budget data (uses USDA crop categories at USDA agricultural regions as spatial unit)
budget = pd.read_excel('data/usda farm budget summary (machine readable).xlsx')

# Load USDA Irrigation Survey data (uses USDA crop categories and States as spatial unit)
irrigation = pd.read_excel('data/usda irrigation summary.xlsx')

# Load USDA Irrigation Water Requirement data (uses USDA crop categories and States as spatial unit)
nir = pd.read_excel('data/usda irrigation water requirement.xlsx')

#nldas_states = pd.read_csv('../../wm abm data/nldas pmp inputs/nldas_states_lookup.txt')

# Load lookup table that geographically associates NLDAS cells, states, and USDA agricultural regions. The table was
# pre-processed by spatial joining shapefiles in GIS
nldas_lookup = pd.read_csv('../data/nldas_states_counties_regions.csv')

# Load USDA Irrigation data on irrigation water by source (groundwater, surface water, off-farm surface water).
# The data is provided at State level.
water_perc = pd.read_csv('../data/water_proportions.csv')

# Load USDA Irrigation data on groundwater costs and surface water costs (at State level)
water_cost = pd.read_csv('../data/water_costs.csv')

#### Step 3 - Conduct Additional Processing of External Data

# For each State, convert irrigation water totals into percents of irrigation water coming from each source
# (groundwater, surface water, or off-farm surface water). For 'D' (no information / did not report) values,
# assume countrywide averages.
for index, row in water_perc.iterrows():
    sum_perc = 0
    total = row['Total']
    if row['Groundwater'] == 'D':
        sum_perc += water_perc[(water_perc['State'] == 'United States')]['Groundwater'].astype('float') / \
                    water_perc[(water_perc['State'] == 'United States')]['Total'].astype('float')
    else:
        total -= float(row['Groundwater'])
    if row['SW (Farm)'] == 'D':
        sum_perc += water_perc[(water_perc['State'] == 'United States')]['SW (Farm)'].astype('float') / \
                    water_perc[(water_perc['State'] == 'United States')]['Total'].astype('float')
    else:
        total -= float(row['SW (Farm)'])
    if row['SW (off-farm)'] == 'D':
        sum_perc += water_perc[(water_perc['State'] == 'United States')]['SW (off-farm)'].astype('float') / \
                    water_perc[(water_perc['State'] == 'United States')]['Total'].astype('float')
    else:
        total -= float(row['SW (off-farm)'])

    if row['Groundwater'] == 'D':
        perc = water_perc[(water_perc['State'] == 'United States')]['Groundwater'].astype('float') / \
               water_perc[(water_perc['State'] == 'United States')]['Total'].astype('float') / sum_perc
        new_value = perc * total / row['Total']
        new_value = new_value.values[0]
    else:
        new_value = float(row['Groundwater']) / row['Total']
    water_perc.set_value(index, 'Groundwater', new_value)
    if row['SW (Farm)'] == 'D':
        perc = water_perc[(water_perc['State'] == 'United States')]['SW (Farm)'].astype('float') / \
               water_perc[(water_perc['State'] == 'United States')]['Total'].astype('float') / sum_perc
        new_value = perc * total / row['Total']
        new_value = new_value.values[0]
    else:
        new_value = float(row['SW (Farm)']) / row['Total']
    water_perc.set_value(index, 'SW (Farm)', new_value)
    if row['SW (off-farm)'] == 'D':
        perc = water_perc[(water_perc['State'] == 'United States')]['SW (off-farm)'].astype('float') / \
               water_perc[(water_perc['State'] == 'United States')]['Total'].astype('float') / sum_perc
        new_value = perc * total / row['Total']
        new_value = new_value.values[0]
    else:
        new_value = float(row['SW (off-farm)']) / row['Total']
    water_perc.set_value(index, 'SW (off-farm)', new_value)

water_perc['SW Total'] = water_perc['SW (off-farm)'] + water_perc['SW (Farm)']

# Merge irrigation water costs and irrigation water source tables
water_perc = pd.merge(water_perc, water_cost,on='State',how='left')

# Calculate adjusted cost for all surface water sources assuming that on-farm surface water is free (do USDA SW costs
# include pumping costs?)
water_perc['SW cost adj'] = (water_perc['SW (off-farm)'] / (water_perc['SW (Farm)'] + water_perc['SW (off-farm)'])) * water_perc['SW cost']

# Merge CDL data with associated geographies (USDA Agricultural Regions and States)
cdl_states = pd.merge(cdl, nldas_lookup[['NLDAS_ID','ERS_region','State','State_Name']],on='NLDAS_ID',how='left')

# Calculate total available arable land for each NLDAS cell using CDL (using year 2010 data). Assumes that
# GCAM categories 'NotAvailable', 'RockIceDesert', and 'UrbanLand' are not available for agricultural use
cdl_states_select_year = cdl_states[(cdl_states['year'] == 2010)]
aggregation_functions = {'value': 'sum'}

cdl_states_total = cdl_states_select_year.groupby(['NLDAS_ID'], as_index=False).aggregate(aggregation_functions)
cdl_states_total = cdl_states_total.set_index('NLDAS_ID')

notavail = cdl_states_select_year[(cdl_states_select_year['GCAM_name']) == 'NotAvailable'].set_index('NLDAS_ID')
notavail = notavail.rename(columns={"value": "notavail"})

rock = cdl_states_select_year[(cdl_states_select_year['GCAM_name']) == 'RockIceDesert'].set_index('NLDAS_ID')
rock = rock.rename(columns={"value": "rock"})

urban = cdl_states_select_year[(cdl_states_select_year['GCAM_name']) == 'UrbanLand'].set_index('NLDAS_ID')
urban = urban.rename(columns={"value": "urban"})

cdl_states_total = pd.merge(cdl_states_total, notavail[['notavail']],left_index=True,right_index=True,how='left')
cdl_states_total = pd.merge(cdl_states_total, rock[['rock']],left_index=True,right_index=True,how='left')
cdl_states_total = pd.merge(cdl_states_total, urban[['urban']],left_index=True,right_index=True,how='left')

cdl_states_total['avail'] = cdl_states_total['value'] - cdl_states_total['urban'] - cdl_states_total['rock'] - cdl_states_total['notavail']

cdl_states_total = cdl_states_total[~cdl_states_total.index.duplicated(keep='first')]

# Extract required data from USDA budget dataframe

# Create new budget table to load data into from source budget table
cols = ['crop', 'region', 'total costs', 'irr water costs', 'yield', 'price', 'opplabor', 'oppland']
items = ['Total, costs listed','Purchased irrigation water','Yield','Price','Opportunity cost of unpaid labor','Opportunity cost of land']
budget_table_lookup = pd.DataFrame(columns=cols)

# For each crop and USDA ag region, loop through the source budget table and extract relevant information for year 2010.
# If data is missing for any specific item, assume United States averages. If United States averages are missing,
# fill in value with a temporary '99999' value
for crop in budget.Commodity.unique():
    for region in budget.Region.unique():
        item_list = [crop, region]
        for i in items:
            if crop == 'Beets' and i == 'Price':
                i = 'Season-average price'
            try:
                item_value = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == 2010)
                       & (budget['Item'] == i)].Value.values[0] ##### Total costs
            except IndexError:

                try:
                    year_max = total_cost = budget[(budget['Commodity'] == crop) & (budget['Region'] == region)].Year.max()
                    item_value = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == year_max)
                       & (budget['Item'] == i)].Value.values[0] ##### Total costs
                except IndexError:
                    try:
                        item_value = \
                        budget[(budget['Commodity'] == crop) & (budget['Region'] == 'U.S. total') & (budget['Year'] == 2010)
                               & (budget['Item'] == i)].Value.values[0]  ##### Total costs
                    except IndexError:
                        try:
                            year_max = total_cost = budget[(budget['Commodity'] == crop) & (budget['Region'] == 'U.S. total')].Year.max()
                            item_value = \
                            budget[(budget['Commodity'] == crop) & (budget['Region'] == 'U.S. total') & (budget['Year'] == year_max)
                                   & (budget['Item'] == i)].Value.values[0]  ##### Total costs
                        except IndexError:
                            item_value = 99999

            item_list.append(item_value)

            # try:
            #     if crop == 'Beets': ##### for Beets, most recent available year of data is 2007
            #         total_cost = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == 2007)
            #                             & (budget['Item'] == "Total, costs listed")].Value.values[0]  ##### Total costs
            # except IndexError:
            #     total_cost = 99999
            #
            # try:
            #     irr_water_cost = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == 2010)
            #            & (budget['Item'] == "Purchased irrigation water")].Value.values[0] ##### Purchased irrigation water
            # except IndexError:
            #     irr_water_cost = 99999
            #
            # try:
            #     yld = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == 2010)
            #            & (budget['Item'] == "Yield")].Value.values[0] ##### Yield
            # except IndexError:
            #     yld = 99999
            #
            # try:
            #     price = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == 2010)
            #            & (budget['Item'] == "Price")].Value.values[0] ##### Yield
            # except IndexError:
            #     price = 99999
            #
            # try:
            #     opplabor = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == 2010)
            #            & (budget['Item'] == "Opportunity cost of unpaid labor")].Value.values[0] ##### Opportunity cost of unpaid labor
            # except IndexError:
            #     opplabor = 99999
            #
            # try:
            #     oppland = budget[(budget['Commodity'] == crop) & (budget['Region'] == region) & (budget['Year'] == 2010)
            #            & (budget['Item'] == "Opportunity cost of land")].Value.values[0] ##### Opportunity cost of land
            # except IndexError:
            #     oppland = 99999

        lst = [item_list]
        table_append = pd.DataFrame(lst, columns=cols)
        budget_table_lookup = budget_table_lookup.append(table_append, ignore_index=True)


# Add in additional irrigation, NIR, and budget data for missing crops from various sources (note: assumes local/state
# sources apply to entire U.S.) PDFs can be found in report folders.

# Add Potato NIR from Western Ag Research doc (table 3), assumed to apply across United States
nir.loc[-1] = ['United States (2013)', 'Potato', 1.67]
# Add in Sugarbeet NIR from Idaho data (table 2 and 3), assumed to apply across United States
nir.loc[-2] = ['United States (2013)', 'Sugarbeet', 4.52] ##### added from idaho data (table 2 and table 3)
# Add in Sugarbeet irrigated area and yield data from Western Ag Research doc (table 3), and USDA documentation
# assume zero non-irrigated area
#irrigation.loc[-1] = ['United States (2013)', 'Potato', 'NA', 414, 'cwt/acre', 'NA', 'NA', 'NA']
irrigation.loc[-2] = ['United States (2013)', 'Sugarbeet', 1113000, 34.5, 'ton/acre', 0, 'NA', 'NA']
# Add in Potato budget data from University of Idaho Survey (2010 southwestern idaho irrigated russet burbank commercial
# potatos: with fumigation and non storage)
budget_table_lookup.loc[-1] = ['Potato', 'U.S. total', 2190, 119.05, 515, 7, 0, 0] ##### added from university of idaho survey
# Add in Sorghum Hay budget data from Ibendahl 2019 report (South Central Kansas)
budget_table_lookup.loc[-2] = ['Sorghum Hay', 'U.S. total', 314.26, 0, 10.90, 24.55, 0, 0] #### added from Ibendahl 2019 report (South Central Kansas)

#### Step 4 - Define Crop Name Mappings between various tables (CDL/GCAM, USDA Irrigation, USDA NIR, USDA Budget)

# Define crop name mappings
crop_name_map = {'corn': {'gcam': 'Corn', 'budget': 'Corn', 'nir': 'Corn for grain or seed', 'irrigation': 'Corn for grain or seed'},
                 'wheat': {'gcam': 'Wheat', 'budget': 'Wheat', 'nir': 'Wheat for grain or seed', 'irrigation': 'Wheat for grain or seed'},
                 'rice': {'gcam': 'Rice', 'budget': 'Rice', 'nir': 'Rice', 'irrigation': 'Rice'},
                 'root_tuber': {'gcam': 'Root_Tuber', 'budget': 'Potato', 'nir': 'Potato', 'irrigation': 'Potatoes, excluding sweet potatoes'},
                 'oil_crop': {'gcam': 'OilCrop', 'budget': 'Soybean', 'nir': 'Soybeans for beans', 'irrigation': 'Soybeans for beans'},
                 'sugar_crop': {'gcam': 'SugarCrop', 'budget': 'Beets', 'nir': 'Sugarbeet', 'irrigation': 'Sugarbeet'},
                 'other_grain': {'gcam': 'OtherGrain', 'budget': 'Sorghum', 'nir': 'Sorghum for grain or seed', 'irrigation': 'Sorghum for grain or seed'},
                 'fiber_crop': {'gcam': 'FiberCrop', 'budget': 'Cotton', 'nir': 'All cotton', 'irrigation': 'All cotton'},
                 'fodder_grass': {'gcam': 'FodderGrass', 'budget': 'Sorghum Hay', 'nir': 'All other hay (dry hay, greenchop, and silage)', 'irrigation': 'All other hay (dry hay, greenchop, and silage)'},
                 #'fodder_herb': {'gcam': 'FodderHerb', 'budget': 'Corn', 'nir': 'Corn for silage or greenchop', 'irrigation': 'Corn for silage or greenchop'},
                 'misc_crop': {'gcam': 'MiscCrop', 'budget': 'Peanut', 'nir': 'Peanuts for nuts', 'irrigation': 'Peanuts for nuts'}
                 }

# If crop group from USDA Irrigation Survey does not match with one of the GCAM categories, reassign to a different
# USDA crop category that will serve as a representative crop
#
# usda_unassigned = {'Peanuts for nuts': ['Beans, dry edible', 'Land in vegetables', 'Sweet corn', 'Tomatoes', 'Lettuce and romaine', 'Land in orchards, vineyards, and nut trees', 'All berries', 'All other crops (see text)'],
#                    'Sorghum for grain or seed': ['Other small grains (barley, oats, rye, etc.)'],
#                    #'Corn for silage or greenchop': ['Alfalfa and alfalfa mixtures (dry hay, greenchop, and silage)'],
#                     'All other hay (dry hay, greenchop, and silage)': ['Pastureland, all types','Corn for silage or greenchop','Alfalfa and alfalfa mixtures (dry hay, greenchop, and silage)'],
#                    }

usda_unassigned = {'Peanuts for nuts': ['Beans, dry edible', 'Land in vegetables', 'Sweet corn', 'Tomatoes', 'Lettuce and romaine', 'Land in orchards, vineyards, and nut trees', 'All berries', 'All other crops (see text)'],
                   'Sorghum for grain or seed': ['Other small grains (barley, oats, rye, etc.)'],
                   #'Corn for silage or greenchop': ['Alfalfa and alfalfa mixtures (dry hay, greenchop, and silage)'],
                    'All other hay (dry hay, greenchop, and silage)': ['Corn for silage or greenchop','Alfalfa and alfalfa mixtures (dry hay, greenchop, and silage)'],
                   }

#### Step 5 - Loop through crops and run table joins, calculations, etc.
first = True
# Initiate for loop for each GCAM crop category
for key2, value in crop_name_map.items():


    # Extract subset of data for crop from the CDL data table

    cdl_states_select = cdl_states[(cdl_states['GCAM_name'] == value['gcam']) & (cdl_states['year'] == 2010)]
    cdl_states_select = cdl_states_select.drop_duplicates()

    # Calculate cropped area by proportion at the state level (State cropped area / Total United States cropped area)
    # and store results in new table (cdl_states_proportion)

    cdl_states_select['cdl_perc'] = 0
    cdl_states_proportion = pd.DataFrame(columns=['state','total_cdl'])
    for state in cdl_states_select.State_Name.unique():
        crop_sum = cdl_states_select[(cdl_states_select['State_Name'] == state)].value.sum()
        if crop_sum != 0:
            cdl_states_select.loc[cdl_states_select.State_Name == state, 'cdl_perc'] = cdl_states_select.value / crop_sum
        lst = [[state, crop_sum]]
        table_append = pd.DataFrame(lst, columns=['state','total_cdl'])
        cdl_states_proportion = cdl_states_proportion.append(table_append, ignore_index=True)
    cdl_states_proportion['state_perc'] = cdl_states_proportion['total_cdl'] / cdl_states_proportion.total_cdl.sum()


    # Join budget table to CDL table

    budget_table_lookup_select = budget_table_lookup[(budget_table_lookup['crop']==value['budget'])]
    cdl_states_merge = pd.merge(cdl_states_select, budget_table_lookup_select[['region','total costs','irr water costs','yield','price','opplabor','oppland']],left_on='ERS_region',right_on='region',how='left')

    # For CDL rows that are missing budget data after the join (99999, null values), replace with U.S. averages
    cdl_states_merge['total costs'] = np.where(cdl_states_merge['total costs'] == 99999,
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['total costs'], cdl_states_merge['total costs'])
    cdl_states_merge['total costs'] = np.where(cdl_states_merge['total costs'].isnull(),
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['total costs'], cdl_states_merge['total costs'])

    cdl_states_merge['irr water costs'] = np.where(cdl_states_merge['irr water costs'] == 99999,
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['irr water costs'], cdl_states_merge['irr water costs'])
    cdl_states_merge['irr water costs'] = np.where(cdl_states_merge['irr water costs'].isnull(),
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['irr water costs'], cdl_states_merge['irr water costs'])

    cdl_states_merge['yield'] = np.where(cdl_states_merge['yield'] == 99999,
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['yield'], cdl_states_merge['yield'])
    cdl_states_merge['yield'] = np.where(cdl_states_merge['yield'].isnull(),
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['yield'], cdl_states_merge['yield'])

    cdl_states_merge['price'] = np.where(cdl_states_merge['price'] == 99999,
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['price'], cdl_states_merge['price'])
    cdl_states_merge['price'] = np.where(cdl_states_merge['price'].isnull(),
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['price'], cdl_states_merge['price'])

    cdl_states_merge['opplabor'] = np.where(cdl_states_merge['opplabor'] == 99999,
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['opplabor'], cdl_states_merge['opplabor'])
    cdl_states_merge['opplabor'] = np.where(cdl_states_merge['opplabor'].isnull(),
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['opplabor'], cdl_states_merge['opplabor'])

    cdl_states_merge['oppland'] = np.where(cdl_states_merge['oppland'] == 99999,
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['oppland'], cdl_states_merge['oppland'])
    cdl_states_merge['oppland'] = np.where(cdl_states_merge['oppland'].isnull(),
                                               budget_table_lookup_select[(budget_table_lookup_select['region']=='U.S. total')]['oppland'], cdl_states_merge['oppland'])

    # Join NIR table to CDL table

    nir_select = nir[(nir['Crop']==value['nir'])]
    cdl_states_merge = pd.merge(cdl_states_merge, nir_select[['Geography','Irrigation (acre-ft/acre)']],left_on='State_Name',right_on='Geography',how='left')

    # For CDL rows that are missing NIR values after the join, fill in with 0 or United States averages where appropriate

    # Replace '-' entries with 0
    cdl_states_merge['Irrigation (acre-ft/acre)'] = np.where(cdl_states_merge['Irrigation (acre-ft/acre)'] == '-',
                                              0, cdl_states_merge['Irrigation (acre-ft/acre)'])
    # Replace '(D)' entries with US average (could not be reported to give away identify of farm)
    cdl_states_merge['Irrigation (acre-ft/acre)'] = np.where(cdl_states_merge['Irrigation (acre-ft/acre)'] == '(D)',
                                              nir_select[(nir_select['Geography']=='United States (2013)')]['Irrigation (acre-ft/acre)'], cdl_states_merge['Irrigation (acre-ft/acre)'])
    # Replace '(NA)' entries with US average (could not be reported to give away identify of farm)
    cdl_states_merge['Irrigation (acre-ft/acre)'] = np.where(cdl_states_merge['Irrigation (acre-ft/acre)'] == 'NA',
                                              nir_select[(nir_select['Geography']=='United States (2013)')]['Irrigation (acre-ft/acre)'], cdl_states_merge['Irrigation (acre-ft/acre)'])
    # Replace '' entries with US average (could not be reported to give away identify of farm)
    cdl_states_merge['Irrigation (acre-ft/acre)'] = np.where(cdl_states_merge['Irrigation (acre-ft/acre)'] == '',
                                              nir_select[(nir_select['Geography']=='United States (2013)')]['Irrigation (acre-ft/acre)'], cdl_states_merge['Irrigation (acre-ft/acre)'])

    # Join Irrigation table to CDL table

    irrigation_select = irrigation[(irrigation['Crop']==value['irrigation'])]
    cdl_states_merge = pd.merge(cdl_states_merge, irrigation_select[['Geography','Area Irrigated (Acres)','Yield Irrigated', 'Area Non-Irrigated (Acres)', 'Yield Non-Irrigated']],
                                left_on='State_Name', right_on='Geography', how='left')
    cdl_states_merge = pd.merge(cdl_states_merge, cdl_states_proportion, left_on='State_Name', right_on='state', how='left')

    # For CDL rows that are missing Irrigation data after the join, fill in with 0 (where appropriate) or a large negative value. The large
    # negative value will indicate that the irrigated and non-irrigated areas needs to be estimated
    keys = ['Area Irrigated (Acres)', 'Area Non-Irrigated (Acres)']

    for key in keys:
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '-', 0, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(Z)', 0, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '', -99999999999, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == 'NA', -99999999999, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(NA)', -99999999999, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(D)', -99999999999, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key].isnull(), -99999999999, cdl_states_merge[key])

    # If there are unassigned USDA crops associated with the current crop selected in the loop, add the irrigated and
    # non-irrigated areas for the unassigned crop
    if value['irrigation'] in usda_unassigned.keys():
        for unassigned_crop in usda_unassigned[value['irrigation']]:
            irrigation_select_unassigned = irrigation[(irrigation['Crop'] == unassigned_crop)]
            for key in keys:
                irrigation_select_unassigned[key] = np.where(irrigation_select_unassigned[key] == '-', 0, irrigation_select_unassigned[key])
                irrigation_select_unassigned[key] = np.where(irrigation_select_unassigned[key] == '(Z)', 0, irrigation_select_unassigned[key])
                irrigation_select_unassigned[key] = np.where(irrigation_select_unassigned[key] == '', -99999999999, irrigation_select_unassigned[key])
                irrigation_select_unassigned[key] = np.where(irrigation_select_unassigned[key] == 'NA', -99999999999, irrigation_select_unassigned[key])
                irrigation_select_unassigned[key] = np.where(irrigation_select_unassigned[key] == '(NA)', -99999999999, irrigation_select_unassigned[key])
                irrigation_select_unassigned[key] = np.where(irrigation_select_unassigned[key] == '(D)', -99999999999, irrigation_select_unassigned[key])
                irrigation_select_unassigned[key] = np.where(irrigation_select_unassigned[key].isnull(), -99999999999, irrigation_select_unassigned[key])
            irrigation_select_unassigned['irrigated add'] = irrigation_select_unassigned['Area Irrigated (Acres)']
            irrigation_select_unassigned['nonirrigated add'] = irrigation_select_unassigned['Area Non-Irrigated (Acres)']
            cdl_states_merge = pd.merge(cdl_states_merge, irrigation_select_unassigned[['Geography','irrigated add','nonirrigated add']],
                                left_on='State_Name', right_on='Geography', how='left')

            cdl_states_merge['Area Irrigated (Acres)'] = cdl_states_merge['Area Irrigated (Acres)'] + cdl_states_merge['irrigated add']
            cdl_states_merge['Area Non-Irrigated (Acres)'] = cdl_states_merge['Area Non-Irrigated (Acres)'] + cdl_states_merge['nonirrigated add']
            cdl_states_merge = cdl_states_merge.drop(columns=['irrigated add', 'nonirrigated add', 'Geography'])

            # cdl_states_merge.to_csv('test_' + unassigned_crop + '.csv') ####!!JY TEST TO SEE IF ADDITIONS WORKING PROPERLY

    # If any representative crop/state is missing irrigated and non-irrigated area values (including any of the
    # unassigned crops assigned to rep crop, we designate the areas as '(NA)' (!JY: is there a better way to implement
    # this that utilizes more of the data?)
    cdl_states_merge['Area Irrigated (Acres)'] = np.where(cdl_states_merge['Area Irrigated (Acres)'] < 0, '(NA)', cdl_states_merge['Area Irrigated (Acres)'])
    cdl_states_merge['Area Non-Irrigated (Acres)'] = np.where(cdl_states_merge['Area Non-Irrigated (Acres)'] < 0, '(NA)', cdl_states_merge['Area Non-Irrigated (Acres)'])

    # For CDL rows that are missing irrigated and non-irrigated areas at the state level, assume that the proportion of
    # CDL State Crop Area / CDL U.S. Total Crop Area is correct and calculate areas
    for key in keys:

        # Subtract areas for Alaska and Hawaii from United States totals (not included in CDL sums)
        if irrigation_select[(irrigation_select['Geography'] == 'Alaska')].empty == False:
            if irrigation_select[(irrigation_select['Geography'] == 'Alaska')][key].values[0] == '(D)' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Alaska')][key].values[0] == 'NA' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Alaska')][key].values[0] == '(NA)' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Alaska')][key].values[0] == '(Z)' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Alaska')][key].values[0] == '-' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Alaska')][key].values[0] == '':
                irrigation_select.loc[irrigation_select['Geography'] == 'Alaska', key] = 0
        else:
            irrigation_select.loc[-3] = ['Alaska', value['irrigation'], 0, 0, 'ton/acre', 0, 0, 'NA']

        if irrigation_select[(irrigation_select['Geography'] == 'Hawaii')].empty == False:
            if irrigation_select[(irrigation_select['Geography'] == 'Hawaii')][key].values[0] == '(D)' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Hawaii')][key].values[0] == 'NA' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Hawaii')][key].values[0] == '(NA)' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Hawaii')][key].values[0] == '(Z)' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Hawaii')][key].values[0] == '-' or \
                    irrigation_select[(irrigation_select['Geography'] == 'Hawaii')][key].values[0] == '':
                irrigation_select.loc[irrigation_select['Geography'] == 'Hawaii', key] = 0
        else:
            irrigation_select.loc[-4] = ['Hawaii', value['irrigation'], 0, 0, 'ton/acre', 0, 0, 'NA']

        united_states_adjusted = irrigation_select[(irrigation_select['Geography'] == 'United States (2013)')][key].values[0] - \
                                 irrigation_select[(irrigation_select['Geography'] == 'Alaska')][key].values[0] - \
                                 irrigation_select[(irrigation_select['Geography'] == 'Hawaii')][key].values[0]

        # !JY: Need to check here if there are times when Alaska or Hawaii are NAN, causing the entire value to be NAN
        if math.isnan(united_states_adjusted):
            united_states_adjusted = 0

        # For any crop/state values that are missing irrigated or non-irrigated areas, calculate based on
        # CDL state / CDL U.S. total proportions
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '-', 0, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(Z)', 0, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(D)',
                                         united_states_adjusted * cdl_states_merge['state_perc'], cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == 'NA',
                                         united_states_adjusted * cdl_states_merge['state_perc'], cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '',
                                         united_states_adjusted * cdl_states_merge['state_perc'], cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(NA)',
                                         united_states_adjusted * cdl_states_merge['state_perc'], cdl_states_merge[key])

    # Calculate adjusted irrigated areas using CDL proportions
    cdl_states_merge['area_irrigated'] = cdl_states_merge['cdl_perc'] * cdl_states_merge['Area Irrigated (Acres)']
    cdl_states_merge['area_nonirrigated'] = cdl_states_merge['cdl_perc'] * cdl_states_merge['Area Non-Irrigated (Acres)']

    # Replace missing yield values from USDA irrigation data with United States averages
    keys = ['Yield Irrigated', 'Yield Non-Irrigated']

    for key in keys:
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '-', 0, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(D)', irrigation_select[(irrigation_select['Geography']=='United States (2013)')][key], cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(NA)', irrigation_select[(irrigation_select['Geography']=='United States (2013)')][key], cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '(Z)', 0, cdl_states_merge[key])
        cdl_states_merge[key] = np.where(cdl_states_merge[key] == '', irrigation_select[(irrigation_select['Geography']=='United States (2013)')][key], cdl_states_merge[key])

    # Concatenate tables into one consolidated table for all crops
    if first:
        cdl_states_all = cdl_states_merge
        first = False
    else:
        cdl_states_all = pd.concat([cdl_states_all, cdl_states_merge])

#### Step X - Identify cells for cropped areas are greater than available area and proportionally re-distribute to other cells in the state
cdl_states_all['area_irrigated_corrected'] = 0
cdl_states_all['area_nonirrigated_corrected'] = 0



first_state = True

for state in cdl_states_all.State.unique():

    if pd.isnull(state):
        continue

    if first_state == True:
        cdl_states_all_subset = cdl_states_all

    aggregation_functions = {'area_irrigated': 'sum','area_nonirrigated': 'sum'}
    cdl_states_test_area = cdl_states_all.groupby(['NLDAS_ID'], as_index=False).aggregate(aggregation_functions)
    cdl_states_test_area['total_area_sqft'] = (cdl_states_test_area['area_irrigated'] + cdl_states_test_area['area_nonirrigated'])*43560
    cdl_states_total_temp = cdl_states_total.reset_index()
    cdl_states_total_temp = pd.merge(cdl_states_total_temp, cdl_states_test_area,left_on='NLDAS_ID',right_on='NLDAS_ID',how='left')
    cdl_states_total_temp['crop_divide_avail'] = cdl_states_total_temp['total_area_sqft'] / cdl_states_total_temp['avail'] # identify those cells with crop area greater than available land area
    cdl_states_total_temp = pd.merge(cdl_states_total_temp, nldas_lookup[['NLDAS_ID', 'State', 'State_Name']], on='NLDAS_ID', how='left') # join table above with state designations

    cdl_states_total_subset = cdl_states_total_temp[(cdl_states_total_temp.State == state)]

    loop_no = 0
    while cdl_states_total_subset.crop_divide_avail.max() > 1:

        loop_no += 1

        cdl_states_total_subset = cdl_states_total_temp[(cdl_states_total_temp.State==state)] # subset state (eventually incorporate into loop)

        print(state)
        print(loop_no)
        print(cdl_states_total_subset.crop_divide_avail.max())
        print('# of cells that are overallocated')
        print(len(cdl_states_total_subset[(cdl_states_total_subset.crop_divide_avail > 1)].index))

        if(loop_no > 50):
            break

        if loop_no == 1:
            cdl_states_all_subset = cdl_states_all[(cdl_states_all.State == state)]  # subset main CDL table by state
            cdl_states_all_subset['area_irrigated_corrected'] = cdl_states_all_subset['area_irrigated']
            cdl_states_all_subset['area_nonirrigated_corrected'] = cdl_states_all_subset['area_nonirrigated']
            cdl_states_total_subset = cdl_states_total_subset.rename(columns={'area_irrigated': 'area_irrigated_corrected_cellsum',
                                                                              'area_nonirrigated': 'area_nonirrigated_corrected_cellsum'})
        if loop_no != 1:
            cdl_states_all_subset = cdl_states_all_subset.drop(['avail','crop_divide_avail','area_irrigated_corrected_cellsum','area_nonirrigated_corrected_cellsum','total_area_sqft'], axis=1)
            cdl_states_total_subset = cdl_states_total_subset.rename(columns={'area_irrigated_corrected': 'area_irrigated_corrected_cellsum',
                                                                              'area_nonirrigated_corrected': 'area_nonirrigated_corrected_cellsum'})

        cdl_states_all_subset = pd.merge(cdl_states_all_subset, cdl_states_total_subset[['NLDAS_ID','avail','crop_divide_avail','area_irrigated_corrected_cellsum','area_nonirrigated_corrected_cellsum', 'total_area_sqft']], left_on='NLDAS_ID',right_on='NLDAS_ID',how='left') # join excess crop areas to main CDL table

        # determine the amount of excess crop area that needs to be re-allocated
        cdl_states_all_subset['area_irrigated_excess'] = np.where(cdl_states_all_subset['crop_divide_avail'] > 1.001, # calculate excess area_irrigated and area_nonirrigated for re-distribution (1.001 to account for rounding errors)
            cdl_states_all_subset['area_irrigated_corrected'] - (cdl_states_all_subset['area_irrigated_corrected'] / cdl_states_all_subset['crop_divide_avail'].where(cdl_states_all_subset.crop_divide_avail !=0, np.nan)), 0)
        cdl_states_all_subset['area_irrigated_excess'] = cdl_states_all_subset['area_irrigated_excess'].fillna(0)
        cdl_states_all_subset['area_nonirrigated_excess'] = np.where(cdl_states_all_subset['crop_divide_avail'] > 1.001, # calculate excess area_irrigated and area_nonirrigated for re-distribution
            cdl_states_all_subset['area_nonirrigated_corrected'] - (cdl_states_all_subset['area_nonirrigated_corrected'] / cdl_states_all_subset['crop_divide_avail'].where(cdl_states_all_subset.crop_divide_avail !=0, np.nan)), 0)
        cdl_states_all_subset['area_nonirrigated_excess'] = cdl_states_all_subset['area_nonirrigated_excess'].fillna(0)

        # determine the amount of additional area that can be allocated to cells with a cushion per crop category, irrigation category
        cdl_states_all_subset['area_irrigated_alloreserve'] = np.where(cdl_states_all_subset['crop_divide_avail'] < 1,
            (cdl_states_all_subset['area_irrigated_corrected']/np.where(cdl_states_all_subset['area_irrigated_corrected_cellsum']+cdl_states_all_subset['area_nonirrigated_corrected_cellsum']==0,np.nan,cdl_states_all_subset['area_irrigated_corrected_cellsum']+cdl_states_all_subset['area_nonirrigated_corrected_cellsum'])) * ((cdl_states_all_subset['avail']- cdl_states_all_subset['total_area_sqft'])/43560), 0)
        cdl_states_all_subset['area_irrigated_alloreserve'] = cdl_states_all_subset['area_irrigated_alloreserve'].fillna(0)

        cdl_states_all_subset['area_nonirrigated_alloreserve'] = np.where(cdl_states_all_subset['crop_divide_avail'] < 1,
            (cdl_states_all_subset['area_nonirrigated_corrected']/np.where(cdl_states_all_subset['area_irrigated_corrected_cellsum']+cdl_states_all_subset['area_nonirrigated_corrected_cellsum']==0,np.nan,cdl_states_all_subset['area_irrigated_corrected_cellsum']+cdl_states_all_subset['area_nonirrigated_corrected_cellsum'])) * ((cdl_states_all_subset['avail']- cdl_states_all_subset['total_area_sqft'])/43560), 0)
        cdl_states_all_subset['area_nonirrigated_alloreserve'] = cdl_states_all_subset['area_nonirrigated_alloreserve'].fillna(0)

        # determine the existing crop areas for cells that have extra available land area
        # cdl_states_all_subset['area_irrigated_wcushion'] = np.where(cdl_states_all_subset['crop_divide_avail'] < 1, cdl_states_all_subset['area_irrigated_corrected'], 0)
        # cdl_states_all_subset['area_nonirrigated_wcushion'] = np.where(cdl_states_all_subset['crop_divide_avail'] < 1, cdl_states_all_subset['area_nonirrigated_corrected'], 0)

        # determine total crop area that needs to be re-distributed and subtract out excess crop areas
        if loop_no == 1:
            aggregation_functions = {'area_irrigated_excess': 'sum','area_nonirrigated_excess': 'sum', 'area_irrigated_alloreserve': 'sum', 'area_nonirrigated_alloreserve': 'sum'}
            # aggregation_functions = {'area_irrigated_excess': 'sum','area_nonirrigated_excess': 'sum', 'area_irrigated_alloreserve': 'sum', 'area_nonirrigated_alloreserve': 'sum',
            #                          'area_irrigated_wcushion': 'sum', 'area_nonirrigated_wcushion': 'sum'}
            crop_redistribute = cdl_states_all_subset.groupby(['GCAM_name'], as_index=False).aggregate(aggregation_functions)
            crop_redistribute = crop_redistribute.rename(columns={'area_irrigated_excess': 'area_irrigated_excess_sum', 'area_nonirrigated_excess': 'area_nonirrigated_excess_sum',
                                                                  'area_irrigated_alloreserve':'area_irrigated_alloreserve_sum', 'area_nonirrigated_alloreserve':'area_nonirrigated_alloreserve_sum'})
            # crop_redistribute = crop_redistribute.rename(columns={'area_irrigated_excess': 'area_irrigated_excess_sum', 'area_nonirrigated_excess': 'area_nonirrigated_excess_sum',
            #                                                       'area_irrigated_allorserve':'area_irrigated_alloreserve_sum', 'area_nonirrigated_alloreserve':'area_nonirrigated_alloreserve_sum',
            #                                                       'area_irrigated_wcushion': 'area_irrigated_wcushion_sum', 'area_nonirrigated_wcushion': 'area_nonirrigated_wcushion_sum'})
            cdl_states_all_subset.loc[cdl_states_all_subset.area_irrigated_excess > 0, 'area_irrigated_corrected'] = cdl_states_all_subset['area_irrigated_corrected'] - cdl_states_all_subset['area_irrigated_excess']
            print(crop_redistribute)

        if loop_no != 1:
            aggregation_functions = {'area_irrigated_alloreserve': 'sum', 'area_nonirrigated_alloreserve': 'sum'}
            # aggregation_functions = {'area_irrigated_alloreserve': 'sum', 'area_nonirrigated_alloreserve': 'sum',
            #                          'area_irrigated_wcushion': 'sum', 'area_nonirrigated_wcushion': 'sum'}
            crop_redistribute_update = cdl_states_all_subset.groupby(['GCAM_name'], as_index=False).aggregate(aggregation_functions)
            crop_redistribute_update = crop_redistribute_update.rename(columns={'area_irrigated_alloreserve':'area_irrigated_alloreserve_sum', 'area_nonirrigated_alloreserve':'area_nonirrigated_alloreserve_sum'})
            for c_2 in crop_redistribute.GCAM_name.unique():
                crop_redistribute.loc[crop_redistribute.GCAM_name==c_2,'area_irrigated_alloreserve_sum'] = crop_redistribute_update[(crop_redistribute_update.GCAM_name==c_2)]['area_irrigated_alloreserve_sum']
                crop_redistribute.loc[crop_redistribute.GCAM_name==c_2,'area_nonirrigated_alloreserve_sum'] = crop_redistribute_update[(crop_redistribute_update.GCAM_name==c_2)]['area_nonirrigated_alloreserve_sum']
            cdl_states_all_subset = cdl_states_all_subset.drop(['area_irrigated_excess_sum', 'area_nonirrigated_excess_sum', 'area_irrigated_alloreserve_sum', 'area_nonirrigated_alloreserve_sum'], axis=1)

        cdl_states_all_subset = pd.merge(cdl_states_all_subset, crop_redistribute, left_on='GCAM_name', right_on='GCAM_name', how='left')

        # loop through crop types and distribute excess crop areas by proportion
        for c in cdl_states_all_subset.GCAM_name.unique():

            # determine the amount of statewide crop excess to assign to individual cells based on existing areas
            cdl_states_all_subset['area_irrigated_excess_allocation'] = 0
            cdl_states_all_subset.loc[cdl_states_all_subset.GCAM_name==c, 'area_irrigated_excess_allocation'] = \
                cdl_states_all_subset['area_irrigated_excess_sum'] * (cdl_states_all_subset['area_irrigated_corrected']/cdl_states_all_subset['area_irrigated_alloreserve_sum'].where(cdl_states_all_subset.area_irrigated_alloreserve_sum != 0, np.nan))
            cdl_states_all_subset['area_irrigated_excess_allocation'] = cdl_states_all_subset['area_irrigated_excess_allocation'].fillna(0)
            cdl_states_all_subset['area_nonirrigated_excess_allocation'] = 0
            cdl_states_all_subset.loc[cdl_states_all_subset.GCAM_name==c, 'area_nonirrigated_excess_allocation'] = \
                cdl_states_all_subset['area_nonirrigated_excess_sum'] * (cdl_states_all_subset['area_nonirrigated_corrected']/cdl_states_all_subset['area_nonirrigated_alloreserve_sum'].where(cdl_states_all_subset.area_nonirrigated_alloreserve_sum != 0, np.nan))
            cdl_states_all_subset['area_nonirrigated_excess_allocation'] = cdl_states_all_subset['area_nonirrigated_excess_allocation'].fillna(0)

            # check whether amounts above are greater than the reserved area for the crops (take the minimum of the two)
            cdl_states_all_subset['area_irrigated_toadd'] = 0
            cdl_states_all_subset.loc[cdl_states_all_subset.GCAM_name==c, 'area_irrigated_toadd'] = np.where(cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_irrigated_excess_allocation'] > cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_irrigated_alloreserve'],
                    cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_irrigated_alloreserve'], cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_irrigated_excess_allocation'])
            cdl_states_all_subset['area_nonirrigated_toadd'] = 0
            cdl_states_all_subset.loc[cdl_states_all_subset.GCAM_name==c, 'area_nonirrigated_toadd'] = np.where(cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_nonirrigated_excess_allocation'] > cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_nonirrigated_alloreserve'],
                    cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_nonirrigated_alloreserve'], cdl_states_all_subset[(cdl_states_all_subset.GCAM_name==c)]['area_nonirrigated_excess_allocation'])

            # add allocated crop areas to cells
            cdl_states_all_subset['area_irrigated_corrected'] = cdl_states_all_subset['area_irrigated_corrected'] + cdl_states_all_subset['area_irrigated_toadd']

            # subtract out allocated crop areas from statewide excess sums
            crop_redistribute.loc[crop_redistribute.GCAM_name==c, 'area_irrigated_excess_sum'] = crop_redistribute['area_irrigated_excess_sum'] - cdl_states_all_subset['area_irrigated_toadd'].sum()
            crop_redistribute.loc[crop_redistribute.GCAM_name==c, 'area_nonirrigated_excess_sum'] = crop_redistribute['area_nonirrigated_excess_sum'] - cdl_states_all_subset['area_nonirrigated_toadd'].sum()

        #Re-check whether cropped areas greater than available land area
        aggregation_functions = {'area_irrigated': 'sum','area_nonirrigated': 'sum','area_irrigated_corrected': 'sum','area_nonirrigated_corrected': 'sum'}
        cdl_states_test_area = cdl_states_all_subset.groupby(['NLDAS_ID'], as_index=False).aggregate(aggregation_functions)
        cdl_states_test_area['total_area_sqft'] = (cdl_states_test_area['area_irrigated_corrected'] + cdl_states_test_area['area_nonirrigated_corrected'])*43560
        cdl_states_total_temp = cdl_states_total.reset_index()
        cdl_states_total_temp = pd.merge(cdl_states_total, cdl_states_test_area,left_on='NLDAS_ID',right_on='NLDAS_ID',how='left')
        cdl_states_total_temp['crop_divide_avail'] = cdl_states_total_temp['total_area_sqft'] / cdl_states_total_temp['avail'] # identify those cells with crop area greater than available land area
        cdl_states_total_temp = pd.merge(cdl_states_total_temp, nldas_lookup[['NLDAS_ID', 'State', 'State_Name']], on='NLDAS_ID', how='left') # join table above with state designations

        print(crop_redistribute)

    if first_state:
        cdl_states_all_replace = cdl_states_all_subset
        crop_redistribute_all = crop_redistribute
        crop_redistribute['State'] = state
        first_state = False
    else:
        cdl_states_all_replace = pd.concat([cdl_states_all_replace, cdl_states_all_subset],ignore_index=True)
        crop_redistribute['State'] = state
        crop_redistribute_all = pd.concat([crop_redistribute_all, crop_redistribute], ignore_index=True)

#!JY restart here! Institute loop (excess areas are still really large, need to check Rice and MiscCrop assignments)


#### Step 6 - Allocate irrigation to groundwater and surface water using statewide averages (!JY: data sources to make better assumptions?)
cdl_states_all = pd.merge(cdl_states_all, water_perc[['State','Groundwater','SW Total', 'GW cost', 'SW cost adj']],left_on='State_Name', right_on='State',how='left')
cdl_states_all['area_irrigated_gw'] = cdl_states_all['area_irrigated'] * cdl_states_all['Groundwater']
cdl_states_all['area_irrigated_sw'] = cdl_states_all['area_irrigated'] * cdl_states_all['SW Total']

#### Step 7 - Check profit calculations and make adjustments
# Calculate perceived costs (i.e., exclude opportunity costs)
cdl_states_all['perceived_cost'] = cdl_states_all['total costs'] - cdl_states_all['opplabor'] - cdl_states_all['oppland']

# For negative profits, adjust total land cost to assume profit is zero
cdl_states_all['profit'] = (cdl_states_all['yield']*cdl_states_all['price']) - cdl_states_all['perceived_cost']
# !JY: consider adding 10 percent to below?
cdl_states_all['perceived_cost_adj'] = np.where(cdl_states_all['profit'] < 1, cdl_states_all['perceived_cost'] + cdl_states_all['profit'] - 1, cdl_states_all['perceived_cost'])
cdl_states_all['profit_adj'] = (cdl_states_all['yield']*cdl_states_all['price']) - cdl_states_all['perceived_cost_adj']
cdl_states_all['land_only_costs'] = cdl_states_all['perceived_cost_adj'] - cdl_states_all['GW cost'] - cdl_states_all['SW cost adj']

# For GW + SW costs greater than total costs, assume statewide average value of (GW + SW) / total
states_adj = cdl_states_all[(cdl_states_all['land_only_costs'] < 0)]
cdl_states_all['gw cost perc'] = cdl_states_all['GW cost'] / cdl_states_all['perceived_cost_adj']
cdl_states_all['sw cost perc'] = cdl_states_all['SW cost adj'] / cdl_states_all['perceived_cost_adj']
states_list = states_adj.State_Name.unique()
cdl_states_all['GW cost adj'] = 99999
cdl_states_all['SW cost adj 2'] = 99999
states_perc = []

for state in states_list:
    gw_cost_perc_df = cdl_states_all[(cdl_states_all['State_Name']==state) & (cdl_states_all['land_only_costs']>=0)]['GW cost'] / cdl_states_all[(cdl_states_all['State_Name']==state) & (cdl_states_all['land_only_costs']>=0)]['perceived_cost_adj']
    sw_cost_perc_df = cdl_states_all[(cdl_states_all['State_Name']==state) & (cdl_states_all['land_only_costs']>=0)]['SW cost adj'] / cdl_states_all[(cdl_states_all['State_Name']==state) & (cdl_states_all['land_only_costs']>=0)]['perceived_cost_adj']
    gw_cost_perc = gw_cost_perc_df.mean()
    sw_cost_perc = sw_cost_perc_df.mean()
    if gw_cost_perc + sw_cost_perc > 1:
        print(state)
    cdl_states_all.loc[(cdl_states_all['land_only_costs']<0) & (cdl_states_all['State_Name']==state), 'GW cost adj'] = gw_cost_perc * cdl_states_all['perceived_cost_adj']
    cdl_states_all.loc[(cdl_states_all['land_only_costs']<0) & (cdl_states_all['State_Name']==state), 'SW cost adj 2'] = sw_cost_perc * cdl_states_all['perceived_cost_adj']

cdl_states_all.loc[(cdl_states_all['GW cost adj']==99999), 'GW cost adj'] = cdl_states_all['GW cost']
cdl_states_all.loc[(cdl_states_all['SW cost adj 2']==99999), 'SW cost adj 2'] = cdl_states_all['SW cost adj']

cdl_states_all['land_only_costs'] = cdl_states_all['perceived_cost_adj'] - cdl_states_all['GW cost adj'] - cdl_states_all['SW cost adj 2']

cdl_states_all['profit_test'] = (cdl_states_all['yield']*cdl_states_all['price']) - cdl_states_all['land_only_costs'] - cdl_states_all['GW cost adj'] - cdl_states_all['SW cost adj 2']

#### Step 8 - Drop nulls, extract relevant columns, and export to csv
# Drop nulls and fill in missing values (99999s)
cdl_states_all = cdl_states_all.dropna(subset=['State_Name'])  # Drop rows without associated state name (outside of US domain)
cdl_states_all.NLDAS_ID.isnull().values.any()

# Extract only relevant columns
cdl_states_final = cdl_states_all[['NLDAS_ID','GCAM_name','CDL_id','value', 'ERS_region', 'State_Name', 'land_only_costs', 'price',
                                     'GW cost adj', 'SW cost adj 2', 'yield', 'Yield Irrigated', 'Yield Non-Irrigated', 'area_irrigated', 'area_nonirrigated']]

# Export to CSV
cdl_states_final.to_csv('cdl_states_final.csv')