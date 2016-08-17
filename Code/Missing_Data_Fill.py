#Import packages
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
from sklearn import cross_validation
from sklearn import linear_model
from sklearn.cross_validation import cross_val_score
import matplotlib.pyplot as plt
#%matplotlib inline
from sklearn import preprocessing
from sklearn.linear_model import LinearRegression

#Read in Initial Build (from Data_Build.py)
cahouseurl = 'https://github.com/crsalviati/CA_Housing/raw/master/Code/ca_house_initial_build.txt'
cahouse = pd.read_csv(cahouseurl, converters={'geotypevalue':str,'county_fips':str})

#Drop some vars with lots of missing data / avoid collinearity
cahouse['nocar'] = cahouse['bicycle'] + cahouse['publictr'] + cahouse['walk']
cahouse = cahouse.drop(['car', 'carpool', 'cartotal', 'bicycle', 'publictr', 'walk', 'athome'], axis=1) #Drop these to avoid double counting and need to drop one to avoid collinearity
cahouse = cahouse.drop(['job_house_low', 'median_income'], axis=1) 
cahouse = cahouse.drop(['poverty_conc', 'traf_fatal', 'livewage_m', 'inf_care'], axis=1)
cahouse['hh_fam'] = cahouse['hh_m']+ cahouse['hh_f']+cahouse['hh_married']
cahouse = cahouse.drop(['hh_m', 'hh_m_child', 'hh_f', 'hh_f_child', 'hh_married', 'hh_married_child', 'hh_nonfam_solo', 'hh_nonfam'], axis=1)

#Drop rows with lots of missing vars. Drop 
cahouse['missing_vars'] = cahouse.isnull().sum(axis=1)
ca_fillmiss = cahouse[cahouse.missing_vars <= 8]
#Create DF of the Zillow vas (LHS) and region geographical identifiers
ca_zill_labels = ca_fillmiss[['placename', 'county', 'regionid',  'placefp', 'geoname', 'zri_sqft', 'hval_sqft']]
#Create DF of all HCI (RHS) vars with missing data that we want to fill. Exclude some with too much missing data or that don't seem important
ca_fillmiss = ca_fillmiss.drop(['hfood_acc', 'day_care', 'violent_crime', 'child_abuse', 'p_trans_acc', 'placename', 
                                'county', 'regionid', 'zri_sqft', 'hval_sqft', 'missing_vars', 'placefp', 'geoname',
                               'Unnamed: 0', 'food_afford', 'poverty_child', 'alc_off', 'alc_tot'], axis=1)

#create df of only vars with complete data. use this to fit regressions to fill in missing
ca_complete = ca_fillmiss.dropna(axis=1, how='any')

#define function that takes in var name, and fills in missing values of that var using linear regression with error, limiting to min/max
def fillinmiss(miss_var):
    to_fill = ca_fillmiss[miss_var]
    combined = pd.concat([ca_complete, to_fill], axis=1)
    ca_nomiss = combined.dropna(how = 'any', subset = [miss_var], inplace = False).copy()

    linreg = LinearRegression()
    X = ca_nomiss.drop([miss_var], axis=1)
    y = ca_nomiss[miss_var]
    linreg.fit(X, y)

    y_min = y.min()
    y_max = y.max()
    y_hat = linreg.predict(X)
    Standard_error = ( sum( (y - y_hat) ** 2 )/(len(y) - 2) ) ** .5
    combined['oz_pred'] = linreg.predict(ca_complete) + np.random.normal(0, Standard_error, len(combined))
    ca_fillmiss[miss_var].fillna(value = combined['oz_pred'], inplace = True)
    ca_fillmiss.loc[(ca_fillmiss[miss_var] < y_min), miss_var] = y_min 
    ca_fillmiss.loc[(ca_fillmiss[miss_var] > y_max), miss_var] = y_max

for i in ca_fillmiss.columns[ca_fillmiss.isnull().any()]:
    fillinmiss(i)
  
#After filling in all missing RHS data, append back on labels nad LHS vars  
ca_filled = pd.concat([ca_fillmiss, ca_zill_labels], axis=1)


#SAVE FINAL BUILD
ca_filled.to_csv('ca_house_missing_filled.txt', encoding='utf-8')