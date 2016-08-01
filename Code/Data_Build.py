import pandas as pd

#READ IN VARIOUS FILES FOR HCI INDICATORS, CLEAN AND MERGE

# HCI - PARKS
#Read in HCI file for Parks Access as hci
hciurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/ParkBeachOpen10_output4-12-13.xlsx'
hci = pd.read_excel(hciurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
hci.columns = [x.lower() for x in hci.columns]
#Keep data at Place level, for Total race only (Most granular level for which data is available for most indicators)
hci = hci[(hci.geotype=="PL") & (hci.race_eth_code == 9)]
#Limit to fields of interest (don't need names, get those from FIPS file below)
hci = hci[['geotypevalue','geoname', 'p_parkacc', 'pop2010']]

#HCI - GINI
#Read in HCI file for Gini Coefficient as temp
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/gini_place_county_region_st3-26-14.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race 2006-2010, limit to fields of interest, and merge with master HCI set
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9) & (temp.reportyear == '2006-2010')]
temp = temp[['geotypevalue', 'gini_index', 'median_hh_income']]
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - TRANSPORTATION
#Read in HCI file for transport to work
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_Transportation2Work_42_CT_PL_CO_RE_ST_12-12-13-revised.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, 2006-2010, limit fields, reshape, and merge with master HCI
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9) & (temp.reportyear == '2006-2010')]
temp = temp[['geotypevalue','reportyear', 'mode', 'pop_total', 'percent']]
temp = temp.pivot(index='geotypevalue', columns='mode', values='percent')
temp.columns = [x.lower() for x in temp.columns]
temp.reset_index(level=0, inplace=True)
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - FOOD AFFORDABILITY
#Read in HCI file for Food Affordability
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/Food_afford_cdp_co_region_ca4-14-13.xls'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'median_income', 'affordability_ratio', 'ave_fam_size'  ]]
temp.rename(columns={'affordability_ratio':'food_afford'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - FOOD ACCESS
#Read in HCI file for Healthy Food Access
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_RetailFoodEnvironment_75_CA_CO_RE_PL_CT_11-15-13.xls'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'mrfei']]
temp.rename(columns={'mrfei':'hfood_acc'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - JOBS/HOUSING
#Read in HCI file for Jobs/Housing Ratio
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_JobHouseRatio_PL-MS-CO-768-4-24-15.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, limit to fields of interest, reshape data
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'strata_level_name', 'ratio']]
temp = temp.pivot(index='geotypevalue', columns='strata_level_name', values='ratio')
temp.rename(columns={'LowWageJobstoAffordableHousing':'job_house_low'},inplace =True)
temp.rename(columns={'TotalJobstoTotalHousing':'job_house_tot'},inplace =True)
temp.reset_index(level=0, inplace=True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - JOBS/EMPLOYED
#Read in HCI file for Jobs/Employed Residents Ratio
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_JobsMatch_PL-MS-CO_769_4-24-15.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest, reshape data
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[temp.strata_level_name_code==21] #Data is stratified by industry; for now just keep total, possibly revisit
temp = temp[['geotypevalue', 'ratio']]
temp.rename(columns={'ratio':'job_match'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI- OZONE
#Read in HCI file for Unhealthy Ozone
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/ozone_zcta_place_co_region_ca4-14-13.xls'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest, reshape data
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'o3_unhealthy_days']]
temp.rename(columns={'o3_unhealthy_days':'ozone'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - PM2.5
#Read in HCI file for PM2.5 Concentration
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/PM25_zcta_place_co_region_ca4-14-13.xls'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'pm25_concentration']]
temp.rename(columns={'pm25_concentration':'pm25_conc'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - HS GRAD
#Read in HCI file for HS Grad Attainment
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/ed_attain_ge_hs_output04-14-13.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[temp.reportyear == '2006-2010']
temp = temp[['geotypevalue', 'p_hs_edatt']]
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - LIVING WAGE
#Read in HCI file for Living Wage
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_Living_Wage_770_PL_CO_RE_CA_9-29-13.xls'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest, reshape data
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'family_type', 'pct_lt_lw']]
temp = temp.pivot(index='geotypevalue', columns='family_type', values='pct_lt_lw')
temp.rename(columns={'MarriedCouple2Children':'livewage_s'},inplace =True)
temp.rename(columns={'SingleMother2Children':'livewage_m'},inplace =True)
temp.reset_index(level=0, inplace=True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - POVERTY
#Read in HCI file for Poverty Rate
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_PovertyRate_754_CT_PL_CO_RE_CA_1-22-14.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest, reshape data
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'reportyear', 'poverty', 'percent']]
#temp = temp.pivot(index='geotypevalue', columns='poverty', values='percent')
temp = pd.pivot_table(temp, index=['geotypevalue', 'reportyear'], columns='poverty', values='percent')
temp.rename(columns={'Child':'poverty_child'},inplace =True)
temp.rename(columns={'Concentrated':'poverty_conc'},inplace =True)
temp.rename(columns={'Overall':'poverty_all'},inplace =True)
temp.reset_index(level=0, inplace=True)
temp.reset_index(level=0, inplace=True)
temp = temp[temp.reportyear=="2006-2010"]
del temp['reportyear']
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - UNEMPLOYMENT
#Read in HCI file for Unemployment Rate
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_Unemployment_290_CA_RE_CO_CD_PL_CT-5-22-14.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest, reshape data
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'reportyear', 'unemployment_rate']]
temp.rename(columns={'unemployment_rate':'unemp_rate'},inplace =True)
temp = temp[temp.reportyear=="2006-2010"]
del temp['reportyear']
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - DAYCARE
#Read in HCI file for Daycare Centers
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_Licensed_Daycare_Centers_760_CA_RE_CO_CD_PL_CT_110215.xlsx'
temp = pd.read_excel(tempurl, sheetname=1, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest, reshape data
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'strata_level_name', 'rate_slots']]
temp = temp.pivot(index='geotypevalue', columns='strata_level_name', values='rate_slots')
temp.rename(columns={'DAY CARE CENTER':'day_care'},inplace =True)
temp.rename(columns={'INFANT CENTER':'inf_care'},inplace =True)
temp.reset_index(level=0, inplace=True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - CRIME
#Read in HCI file for Violent Crime
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_Crime_752_PL_CO_RE_CA_2000-2013_21OCT15.xlsx'
temp = pd.read_excel(tempurl, sheetname=1, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)] 
temp = temp[temp.strata_level_name_code == 5] #Data is stratified by crime type, but for now just keep total, possibly revisit
temp = temp[temp.reportyear == 2010]
temp = temp[['geotypevalue', 'rate']]
temp.rename(columns={'rate':'violent_crime'},inplace =True)
temp.drop_duplicates('geotypevalue', keep=False, inplace=True) #4 place codes are duplicated, unclear why, drop
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - ABUSE
#Read in HCI file for Child Abuse/Neglect
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_AbuseNeglectChildren_741_CT_PL_CO_RE_CA-24-4-15.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)] 
temp = temp[['geotypevalue', 'percent']]
temp.rename(columns={'percent':'child_abuse'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - VOTERS
#Read in HCI file for Voter Participation
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_RegisteredVoters_653_CA_CO_RE_PL_CT_5-1-14.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9) & (temp.type == 'voted/registered')] #Revisit to estimate registered voter percent?
temp = temp[temp.reportyear == 2010]
temp = temp[['geotypevalue', 'percent']]
temp.rename(columns={'percent':'voted'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - DRINKING WATER
#Read in HCI file for Unsafe Drinking Water (pt 1)
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_DrinkingWater_CA_RE_CO_CD_PL_CT-A-N-6-19-14.xlsx' 
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
#Read in HCI file for Unsafe Drinking Water (pt 2)
temp2url = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_DrinkingWater_CA_RE_CO_CD_PL_CT-O-Y-6-19-14.xlsx'
temp2 = pd.read_excel(temp2url, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp = temp.append(temp2)
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9) & (temp.category == 'total')] #Stratified by violation type, but just keep total. Revisit?
temp = temp[['geotypevalue', 'percent']]
temp.rename(columns={'percent':'bad_water'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - TRANSIT
#Read in HCI file for Public Transit Access (pt 1)
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/RailFerryBus10_SCAG_Output9-5-13.xls'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
#Read in HCI file for Public Transit Access (pt 2)
temp2url = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/RailFerryBus10_SANDAG_Output8-29-13.xls'
temp2 = pd.read_excel(temp2url, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
#Read in HCI file for Public Transit Access (pt 3)
temp3url = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/RailFerryBus10_MTC_Output_11-15-13.xls'
temp3 = pd.read_excel(temp3url, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
#Read in HCI file for Public Transit Access (pt 4)
temp4url = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/RailFerryBus10_SACOG_Output-11-26-13.xls'
temp4 = pd.read_excel(temp4url, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp = temp.append(temp2)
temp = temp.append(temp3)
temp = temp.append(temp4)
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp[['geotypevalue', 'p_trans_acc']]
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - TRAFFIC INJURY
#Read in HCI file for Traffic Injuries (pt 1)
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_RoadTrafficInjuries_753_CT_PL_CO_RE_R4_CA-12-17-13_A-N.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
#Read in HCI file for Traffic Injuries (pt 2)
temp2url = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_RoadTrafficInjuries_753_CT_PL_CO_12-17-13_O-Y.xlsx'
temp2 = pd.read_excel(temp2url, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp = temp.append(temp2)
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp['mode'] == 'All modes')] #Stratified by mode, just keep total for now. Revisit?
temp = pd.pivot_table(temp, index=['geotypevalue', 'reportyear'], columns='severity', values='poprate')
temp.head()
temp.reset_index(level=0, inplace=True)
temp.reset_index(level=0, inplace=True)
temp = temp[temp.reportyear == '2006-2010']
temp.rename(columns={'Killed':'traf_fatal'},inplace =True)
temp.rename(columns={'Severe Injury':'traf_sev'},inplace =True)
del temp['reportyear']
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - ALCOHOL
#Read in HCI file for Alcohol Proximity (pt 1)
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_AlcoholOutletsQ_774_CA_RE_CO_CD_PL_CT-A-N-5-16-14.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
#Read in HCI file for Alcohol Proximity (pt 2)
temp2url = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_AlcoholOutletsQ_774_CO_CD_PL_CT-O-Y-5-16-14.xlsx'
temp2 = pd.read_excel(temp2url, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp = temp.append(temp2)
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)]
temp = temp.pivot(index='geotypevalue', columns='license_type', values='percent')
temp.head()
temp.reset_index(level=0, inplace=True)
temp.rename(columns={'Off_sale':'alc_off'},inplace =True)
temp.rename(columns={'On_sale':'alc_on'},inplace =True)
temp.rename(columns={'Total_licenses':'alc_tot'},inplace =True)
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#HCI - HOUSEHOLD TYPE
#Read in HCI file for Household Type
tempurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/HCI_HouseholdType_746_CARECOCDPLCT_Alameda-Monterey_24APR15.xlsx'
temp = pd.read_excel(tempurl, sheetname=0, converters={'geotypevalue':str,'county_fips':str})
temp.columns = [x.lower() for x in temp.columns]
#Keep Place level data for all race, explore years available, limit to fields of interest
temp = temp[(temp.geotype=="PL") & (temp.race_eth_code == 9)] #Stratified by mode, just keep total for now. Revisit?
temp = pd.pivot_table(temp, index=['geotypevalue', 'reportyear'], columns='strata_level_name', values='households_percent')
temp.reset_index(level=0, inplace=True)
temp.reset_index(level=0, inplace=True)
temp.rename(columns={'Female householder, no husband present':'hh_f'},inplace =True)
temp.rename(columns={'Female householder, no husband present, with own children under 18 years':'hh_f_child'},inplace =True)
temp.rename(columns={'Male householder, no wife present':'hh_m'},inplace =True)
temp.rename(columns={'Male householder, no wife present, with own children under 18 years':'hh_m_child'},inplace =True)
temp.rename(columns={'Married couple':'hh_married'},inplace =True)
temp.rename(columns={'Married couple with own children under 18 years':'hh_married_child'},inplace =True)
temp.rename(columns={'Nonfamily households':'hh_nonfam'},inplace =True)
temp.rename(columns={'Nonfamily households with householder living alone':'hh_nonfam_solo'},inplace =True)
temp = temp[temp.reportyear == '2006-2010']
del temp['reportyear']
#Merge onto combined HCI df
hci = pd.merge(hci, temp, on='geotypevalue', how='outer')

#FIPS CODES
# Read in FIPS codes  and limit to codes for CA
fips = pd.read_csv('https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/fips_codes.txt', sep='|',converters={1:str,2:str})
fips.columns = [x.lower() for x in fips.columns]
fips = fips[(fips.state=="CA")]
fips = fips[['placefp', 'placename', 'county']]
#Merge FIPS codes onto HCI indicators
hci.rename(columns={'geotypevalue':'placefp'},inplace =True)
hci = pd.merge(hci, fips, on='placefp', how='outer')
hci = hci[hci.placename.notnull()] #Some HCI files have a few place codes that don't show up in the master FIPS file. Drop these

#READ IN FILES FOR ZILLOW REAL ESTATE PRICES

#ZILLOW - HOME VALUE
#Read in Zillow index: Median ZRI Per Sq Ft: SFR, Condo/Co-op ($)
zri_sqft = pd.read_csv('https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/City_ZriPerSqft_AllHomes.csv', converters={1:str})
zri_sqft.columns = [x.lower() for x in zri_sqft.columns]
#Limit to observations for CA, keep only vars of interest, and rename vars
zri_sqft = zri_sqft[zri_sqft.state == 'CA']
zri_sqft = zri_sqft[['regionid', 'regionname', 'countyname', '2010-12']]
zri_sqft.rename(columns={'2010-12':'zri_sqft'},inplace =True)

#ZILLOW - RENTAL PRICE
#Read in Zillow index: Median Home Value Per Sq Ft ($)
hval_sqft = pd.read_csv('https://github.com/crsalviati/CA_Housing/raw/master/Raw_Data/City_MedianValuePerSqft_AllHomes.csv', converters={1:str})
hval_sqft.columns = [x.lower() for x in hval_sqft.columns]
hval_sqft.head()
#Limit to observations for CA, keep only vars of interest, and rename vars
hval_sqft = hval_sqft[hval_sqft.state == 'CA']
hval_sqft = hval_sqft[['regionid', 'regionname', 'countyname', '2010-12']]
hval_sqft.rename(columns={'2010-12':'hval_sqft'},inplace =True)
#Merge with home value to make master zillow set
zillow = pd.merge(zri_sqft, hval_sqft, on=['regionid', 'regionname', 'countyname'], how='outer')
zillow.rename(columns={'regionname':'placename'},inplace =True)
zillow.rename(columns={'countyname':'county'},inplace =True)

#MERGE HCI AND ZILLOW

#Merge is done by place name. Need to clean up/standardize some names before merge
hci['placename'] = hci['placename'].str.replace(' city', '')
hci['placename'] = hci['placename'].str.replace(' CDP', '')
hci['placename'] = hci['placename'].str.replace(' town', '')
hci['county'] = hci['county'].str.replace(' County', '')
hci.placename.replace('San Buenaventura (Ventura)', 'Ventura', inplace=True)
hci.placename.replace('El Paso de Robles (Paso Robles)', 'Paso Robles', inplace=True)
hci.placename.replace('Avilla Beach', 'Avila Beach', inplace=True)
hci.placename.replace('Coultervillle', 'Coulterville', inplace=True)
hci.placename.replace('Blairsden|Graeagle', 'Blairsden-Graeagle', inplace=True, regex=True)
hci.placename.replace('St. Helena', 'Saint Helena', inplace=True)
hci.loc[hci.placename == 'Aromas', 'county'] = "Monterey"
hci.loc[hci.placename == 'Tahoma', 'county'] = "El Dorado"
zillow.loc[zillow.placename == 'Westlake Village', 'county'] = "Los Angeles"
zillow.loc[zillow.placename == 'Bradley', 'county'] = "Monterey"
zillow.loc[zillow.placename == 'Cottonwood', 'county'] = "Shasta"
zillow.loc[zillow.placename == 'Maricopa', 'county'] = "Kern"
zillow.loc[zillow.placename == 'Smartsville', 'county'] = "Yuba"
zillow.loc[zillow.placename == 'Trona', 'county'] = "Inyo"
zillow.loc[zillow.placename == 'Westwood', 'county'] = "Lassen"
zillow.placename.replace('Broadmoor Village', 'Broadmoor', inplace=True)
zillow.placename.replace('Carmel', 'Carmel-by-the-Sea', inplace=True)
zillow.placename.replace('Carmel Valley', 'Carmel Valley Village', inplace=True)
zillow.placename.replace('Hilmar', 'Hilmar-Irwin', inplace=True)
zillow.placename.replace('Idyllwild', 'Idyllwild-Pine Cove', inplace=True)
zillow.placename.replace('Lagunitas', 'Lagunitas-Forest Knolls', inplace=True)
zillow.placename.replace('Mather Air Force Base', 'Mather', inplace=True)
zillow.placename.replace('Mc Farland', 'McFarland', inplace=True)
zillow.placename.replace('Mi Wuk Village', 'Mi-Wuk Village', inplace=True)
zillow.placename.replace('Newport Coast', 'Newport Beach', inplace=True)
zillow.placename.replace('Palos Verdes Peninsula', 'Palos Verdes Estates', inplace=True)
zillow.placename.replace('South Yuba City', 'Yuba City', inplace=True)
zillow.placename.replace('Tahoe City', 'Sunnyside-Tahoe City', inplace=True)
zillow.placename.replace('The Sea Ranch', 'Sea Ranch', inplace=True)
zillow.placename.replace('Tuolumne', 'Tuolumne City', inplace=True)
#Merge into final set
fullbuild = pd.merge(hci, zillow, on=['placename', 'county'], how='inner')

#SAVE FINAL BUILD
fullbuild.to_csv('ca_house_initial_build.txt', encoding='utf-8')