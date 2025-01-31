
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

    #rename countries in hdi_df so consistent in all three datasets for clean merge
    hdi_df['Country'] = hdi_df['Country'].replace({"Bolivia (Plurinational State of)":"Bolivia",
                                              "Congo":"Congo (Rep)",
                                              "Congo (Democratic Republic of the)":"Congo (Dem Rep)",
                                              "Czechia":"Czech Republic",
                                              "Côte d'Ivoire":"Cote d'Ivoire",
                                              "Eswatini (Kingdom of)":"Swaziland",
                                              "Hong Kong, China (SAR)":"Hong Kong",
                                              "Iran (Islamic Republic of)":"Iran",
                                              "Korea (Rep)":"South Korea",
                                              "Lao People's Democratic Republic":"Laos",
                                              "Micronesia (Federated States of)":"Micronesia (Fed States)",
                                              "Moldova (Republic of)":"Moldova",
                                               "Russian Federation":"Russia",
                                              "Saint Lucia":"St. Lucia",
                                              "Tanzania (United Republic of)":"Tanzania",
                                              "The former Yugoslav Republic of Macedonia":"North Macedonia",
                                              "Venezuela (Bolivarian Republic of)":"Venezuela",
                                              "Viet Nam":"Vietnam"})

    #rename countries in power_df  so consistent in all three datasets for clean merge
    power_df['country_long'] = power_df['country_long'].replace({"Cape Verde":"Cabo Verde", 
                                                             "Congo":"Congo (Rep)",
                                                             "Cote DIvoire":"Cote d'Ivoire",
                                                             "Democratic Republic of the Congo":"Congo (Dem Rep)",
                                                             "Macedonia":"North Macedonia",
                                                             "United States of America":"United States"})
   
    #rename countries in pop_df so consistent in all three datasets for clean merge
    pop_df['Country'] = pop_df['Country'].replace({"Congo, Dem. Rep.":"Congo (Dem Rep)", 
                                               "Congo, Rep.":"Congo (Rep)",
                                              "Egypt, Arab Rep.":"Egypt",
                                              "Micronesia, Fed. Sts.":"Micronesia (Fed States)",
                                               "Gambia, The":"Gambia",
                                               "Hong Kong SAR, China":"Hong Kong",
                                               "Iran, Islamic Rep.":"Iran",
                                               "Korea, Rep.":"South Korea",
                                               "Lao PDR":"Laos",
                                               "Korea, Dem. People’s Rep.":"North Korea",
                                               "Russian Federation":"Russia",
                                               "Slovak Republic":"Slovakia",
                                               "Venezuela, RB":"Venezuela",
                                               "Yemen, Rep.":"Yemen"
                                              })

     # define dictionary for country region look up
    regions = {'North America':["Canada","Mexico","US"],
                'South/Central America': ["Argentina","Brazil", "Chile", "Colombia", "Ecuador",                                   "Peru","Trinidad & Tobago", "Venezuela","Central America","Other Caribbean",                              "Other South America"], 
               'Europe':["Austria","Belgium","Bulgaria","Croatia","Cyprus","Czech Republic",                                "Denmark", "Estonia","Finland","France", "Germany","Greece","Hungary",                                  "Iceland", "Ireland","Italy",  "Latvia","Lithuania" ,"Luxembourg", "Netherlands",                         "North Macedonia", "Norway", "Poland","Portugal","Romania",                                                "Slovakia","Slovenia","Spain","Sweden" ,"Switzerland",                                                "Turkey", "Ukraine", "United Kingdom", "Other Europe"],
           'CIS':["Azerbaijan","Belarus","Kazakhstan","Russian Federation", "Turkmenistan","USSR", "Uzbekistan", "Other CIS"],
           'Middle East':["Iran","Iraq","Israel","Kuwait","Oman","Qatar", "Saudi Arabia", "United Arab Emirates", "Other Middle East"],
           'Africa':["Algeria","Egypt","Morocco","South Africa","Eastern Africa","Middle Africa", "Western Africa","Other Northern Africa" , "Other Southern Africa"],
           'Asia Pacific':["Australia","Bangladesh","China","China Hong Kong SAR", "India","Indonesia", "Japan","Malaysia","New Zealand","Pakistan","Philippines","Singapore", "South Korea","Sri Lanka","Taiwan","Thailand","Vietnam","Other Asia Pacific"]
                       }
    
    # functions takes a dataframe with a 'Country' column and outputs with added 'Region' column
    def label_regions(df):
        df['Region'] = ""
        for region, countries in regions.items():
            df.loc[df["Country"].isin(countries),"Region"]=region
        return df
    
    #import, clean and format BP consumption data
    consumption_import = pd.read_excel (r'data/bp-stats-review-2019-all-data.xlsx', sheet_name='Primary Energy Consumption',skiprows=2)
    consumption_import = consumption_import.rename(columns={"Million tonnes oil equivalent":"Country"})
    
    label_regions(consumption_import)

    consumption_import = consumption_import.drop(['2018.1', '2007-17', '2018.2','Unnamed: 58','Unnamed: 59'], axis=1)
    consumption = consumption_import[consumption_import["Country"].notnull()].sort_values("Country")
    consumption = consumption[consumption.Region != ""]
    Region = consumption.Region
    consumption.drop(labels=["Region"],axis=1,inplace=True)
    consumption.insert(1,"Region",Region)
    #consumption.head()

    #import and format BP Consumption by Fuel Type data (row cleanup accomplished through merge in next cell)
    consump_by_fuel_import = pd.read_excel (r'data/bp-stats-review-2019-all-data.xlsx', 
                                            sheet_name='Primary Energy - Cons by fuel',skiprows=2)
    consump_by_fuel_import
    consump_by_fuel = consump_by_fuel_import.rename(columns={"Million tonnes oil equivalent":"Country","Oil":"2017 Oil",
                                                   "Natural Gas":"2017 Nat Gas","Coal":"2017 Coal",
                                                   "Nuclear energy":"2017 Nuclear","Hydro electric":"2017 Hydro",
                                                   "Renew- ables":"2017 Renewables",
                                                   "Oil.1":"2018 Oil","Natural Gas.1":"2018 Nat Gas","Coal.1":"2018 Coal",
                                                   "Nuclear energy.1":"2018 Nuclear","Hydro electric.1":"2018 Hydro",
                                                   "Renew- ables.1":"2018 Renewables",
                                                   "Change Oil":"% Change Oil",
                                                   "Change Natural Gas":"% Change Nat Gas",
                                                   "Change Coal":"% Change Coal",
                                                   "Change Nuclear energy":"% Change Nuclear",
                                                   "Change Hydro electric":"% Change Hydro",
                                                   "Change Renew- ables":"% Change Renewables"})
    consump_by_fuel = consump_by_fuel.drop(['Unnamed: 15','Total','Total.1'], axis=1)
    #consump_by_fuel.head()

    #merge consumption df with consump_by_fuel (left merge to finish cleanup of consump_by_fuel)
    tt = consump_by_fuel.melt(id_vars=['Country'], var_name='Year', value_name='energy')
    vals = tt['Year'].str.split(" ", n = 1, expand = True)
    tt['Year']=vals[0]
    tt['Fuel']=vals[1]
    ts=consumption.melt(id_vars=['Country','Region'], var_name='Year',value_name='total_gen')
    ts=ts.astype({'Year':'str'})
    all_consump_data=pd.merge(tt,ts,on=['Country','Year'], how='right')
    all_consump_data['energy']=11.96*all_consump_data['energy']
    all_consump_data['total_gen']=11.96*all_consump_data['total_gen']
    all_consump_data = all_consump_data.drop(columns=['total_gen'])
    
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

    # df for cap added per year
    cap_added_yr = cap_cont_yr.rename(columns={'country_long':'Country', 'commissioning_year':'Year', 'capacity_mw':'cap_added'})
    cap_added_yr['Year'] = cap_added_yr['Year'].astype(str)

    # merge hdi, pop, and capacity by Country and Year
    hdi_pop_merged = pd.merge(hdi_df, pop_df, on=['Country', 'Year'])
    merged_data = pd.merge(hdi_pop_merged, cap_cum, left_on=['Country', 'Year'], right_on=['country_long', 'commissioning_year'])
    merged_data['Population'] = merged_data['Population']/1_000_000
    merged_data = pd.merge(merged_data, cap_added_yr, on=['Country', 'Year'])
    
    # add regions to merged_data
    label_regions(merged_data)
    
    # merged_data = pd.merge(merged_data, all_consump_data)
    return merged_data, all_consump_data