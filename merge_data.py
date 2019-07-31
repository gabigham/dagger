
# Downloads data from sites for power, HDI and population, merges, cleans
# and returns pandas dataframe for analysis

def merge_data():
   
    import numpy as np
    import pandas as pd
    import os
    
    #import, clean and format HI data
    hdi_file = 'data/Human Development Index (HDI).csv'
    hdi_df = pd.read_csv(hdi_file, encoding = "ISO-8859-1", skiprows=1)
    columns = ["Country"]

    for i in range(1990, 2018):
        columns = columns + [str(i)]

    hdi_df = hdi_df[columns]
    hdi_df = hdi_df.melt(id_vars='Country', var_name='Year', value_name="HDI")
    hdi_df = hdi_df.dropna()
    hdi_df['Country'] = hdi_df['Country'].str.strip()


    #import and clean the power data
    power_file = 'data/global_power_plant_database.csv'
    power_df = pd.read_csv(power_file)
    power_df['commissioning_year'] = round(power_df['commissioning_year'],0)
    power_df = power_df[power_df['commissioning_year']>=1990]

    #importing, cleaning, and formatting the population data
    pop_file = ('data/API_SP.POP.TOTL_DS2_en_csv_v2_103676.csv')
    pop_df = pd.read_csv(pop_file, encoding = "ISO-8859-1", skiprows=4)
    pop_df = pop_df.drop(['Country Code', 'Indicator Name', 'Indicator Code'], axis=1)
    pop_df = pop_df.melt(id_vars='Country Name', var_name='Year', value_name='Population')
    pop_df = pop_df.rename(columns={'Country Name':'Country'})

    #filter power df and create df for cumulative capacity
    pow_pd = power_df[['country_long', 'capacity_mw', 'commissioning_year' ]]
    pow_pd.head(20)
    country_list = pow_pd['country_long'].unique()

    cap_data = pd.DataFrame(columns = ['country_long', 'commissioning_year'])

    for i in range(len(country_list)):
        for year in range(1990, 2018):
            cap_data.loc[i*37 + year-1990] = [country_list[i], year]    

    # Aggregate capacity when same country and year
    countries = pow_pd.groupby(['country_long', 'commissioning_year'])
    cap_added = countries['capacity_mw'].sum()

    # merge data agreggate capacity with counrty year df
    cap_cont_yr = pd.merge(cap_data, cap_added, on=['country_long', 'commissioning_year'], how='left')
    cap_cont_yr = cap_cont_yr.fillna(0)

    # create cumulative cap by country and year
    cap_cont_yr['commissioning_year'] = cap_cont_yr['commissioning_year'].astype(str)
    cap_cum = cap_cont_yr.groupby(by=['country_long', 'commissioning_year']).sum().groupby(level=[0]).cumsum()

    # merge hdi, pop, and capacity by Country and Year
    hdi_pop_merged = pd.merge(hdi_df, pop_df, on=['Country', 'Year'])
    merged_data = pd.merge(hdi_pop_merged, cap_cum, left_on=['Country', 'Year'], right_on=['country_long', 'commissioning_year'])

    return merged_data



