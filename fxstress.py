import pandas as pd
import numpy as np
import quandl


from datetime import datetime, timedelta, date


#### Settings ####

# length of time series (years) to consider
data_years = 10

# Quandl API key
quandl.ApiConfig.api_key =  # "API key goes here!"

# get date for end of last quarter
now = datetime.now()
qtr = int ( np.floor( (now.month - 1) / 4.0) )
end_date = date(now.year,3*qtr + 1,1) - timedelta(days=1)


# 10 years previously (assumes not 29/02 on a leap year- if using quarter end dates it is fine)
start_date = date(end_date.year-data_years, end_date.month, end_date.day) + timedelta(days=1)

# We actually need more data to calculate historic changes at start_date!
start_date_data = start_date - timedelta(days=366) # implicitly assumes liquidity <=1 year

# Read CSV file defining all BOE currency codes on Quandl
currencies = pd.read_csv("~/fxstress/quandl_BOE_FX_codes.csv")

# Force fresh data download but can also use locally cached data
# input_data = pd.read_csv("~/fxstress/quandl_BOE_FX_data.csv.gz",index_col=0)
input_data = None

# Download fresh data
if input_data is None:
    input_data = pd.DataFrame(index=pd.date_range(start_date_data, end_date, freq='D')) #frequency="daily"))
    for index, currency in currencies.iterrows():
        # print currency['Label'], currency['Code'], currency['Quandl_Code']
        if not currency['Enabled']: continue # skip these currencies
        print ('Downloading data for', currency['Label'], 'from', currency['Source'])
        fxrates = quandl.get(currency['Source']+"/"+currency['Quandl_Code'], start_date=start_date_data, end_date=end_date, frequency="daily")
        fxrates.rename( columns = {'Value':currency['Code']+'USD'}, inplace = True)
        input_data = pd.merge(input_data, fxrates, how='left', left_index=True, right_index=True) # merge into downloaded data
    # Save to disk to avoid redownloading during development
    #input_data.to_csv("~/fxstress/quandl_BOE_FX_data.csv")
    input_data.to_csv("~/fxstress/quandl_BOE_FX_data.csv.gz",compression="gzip") # when datasets get massive!



# Compute percentile of abs ln change in a pandas time (date) series 
# The time series should contain entries at least beginning at start_date - liquidity days!
def abs_ln_change_pc(fxrates,pc,liquidity_days,start_date):
    # Create lagged version and interpolate missing values (at most 3 days = weekend + bank holidays)
    fxrates_lag = fxrates.shift(liquidity_days)
    fxrates_lag.interpolate(inplace=True, limit=3)
    
    # Get abs (natural) log change
    fxrates_abs_ln_change = abs( np.log(fxrates/fxrates_lag) )
    # return percentile
    return np.nanpercentile(fxrates_abs_ln_change.loc[start_date:], pc)


# Add auxilliary column for USD
input_data['USDUSD'] = 1
currencies_x = currencies.append(pd.DataFrame([['US dollar', 'USD', 'N/A','N/A','USD',1]], columns=currencies.columns), ignore_index=True)
# restrict to enabled currencies
currencies_x = currencies_x[currencies_x.Enabled == 1]
# sort to make presentation nicer
currencies_x.sort_values('Code',inplace=True)

# Calls 
def abs_ln_change_pc_all(currencies_x,pc,liquidity_days,input_data):
    # matrix for the results
    res = pd.DataFrame(index=pd.Index( currencies_x['Code'] ), columns=pd.Index( currencies_x['Code'] ))
    for i, curr_i in currencies_x.iterrows():
        for j, curr_j in currencies_x.iterrows():
            # print i, curr_i['Code'], j, curr_j['Code']
            if curr_i['Code'] < curr_j['Code']:
                res[curr_i['Code']][curr_j['Code']] = abs_ln_change_pc(
                    input_data[curr_i['Code']+'USD']/input_data[curr_j['Code']+'USD'],pc, liquidity_days, start_date)
            elif curr_i['Code'] > curr_j['Code']: # already calculated!
                res[curr_i['Code']][curr_j['Code']] = res[curr_j['Code']][curr_i['Code']]
                
    return res

# liquidity period (in days)

for liquidity_months in [1,3,6,9,12]:
    liquidity_days = int(liquidity_months * 365.25/12.0)
    # print liquidity_months, liquidity_days
    fx_stress_results = abs_ln_change_pc_all(currencies_x,95,liquidity_days,input_data)
    fx_stress_results['GBP']['USD']
    fx_stress_results.to_csv("~/fxstress/quandl_FX_stress_"+str(liquidity_days)+"_day.csv")

