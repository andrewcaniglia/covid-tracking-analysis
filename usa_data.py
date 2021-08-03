import pandas as pd
from datetime import datetime
from functools import reduce

def usa_data():
    
    #pulls official case/death data from the CDC website
    cases = pd.read_csv('https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD')
    cases['date'] = [datetime.strptime(x, '%m/%d/%Y') for x in cases['submission_date']]
    cases.drop('submission_date', axis=1, inplace=True)
    cases['cfr'] = cases['new_death']/cases['new_case']*100
    
    #pulls official hospitalization data from healthdata.gov 
    hosp = pd.read_csv('https://beta.healthdata.gov/api/views/g62h-syeh/rows.csv?accessType=DOWNLOAD').sort_values('date', ascending=False)
    hosp['date'] = [datetime.strptime(x, '%Y/%m/%d') for x in hosp['date']]
    
    #pulls official testing data from healthdata.gov 
    test = pd.read_csv('https://beta.healthdata.gov/api/views/j8mb-icvb/rows.csv?accessType=DOWNLOAD').sort_values(by='date',ascending=False)
    test['date'] = [datetime.strptime(x, '%Y/%m/%d') for x in test['date']]
    test = test.pivot(index=['date', 'state'], columns = 'overall_outcome', values = ['new_results_reported', 'total_results_reported'])
    test.reset_index(inplace=True)

    #Obtains a dataframe of state abbreviations in order to work with census data.
    state_abbv = pd.read_html('https://worldpopulationreview.com/states/state-abbreviations')[0]
    
    #Obtains census data for each state by age group, merges all of these datasets together for a new dataframe.
    census_by_age = pd.read_csv('https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/asrh/sc-est2019-agesex-civ.csv')
    plus18 = census_by_age[(census_by_age['SEX'] == 0) & (census_by_age['AGE']>= 18)].groupby('NAME').sum().reset_index()[['NAME','POPEST2019_CIV']].rename(columns = {'POPEST2019_CIV': '18PlusPop'})
    plus12 = census_by_age[(census_by_age['SEX'] == 0) & (census_by_age['AGE']>= 12)].groupby('NAME').sum().reset_index()[['NAME','POPEST2019_CIV']].rename(columns = {'POPEST2019_CIV': '12PlusPop'})
    plus65 = census_by_age[(census_by_age['SEX'] == 0) & (census_by_age['AGE']>= 65)].groupby('NAME').sum().reset_index()[['NAME','POPEST2019_CIV']].rename(columns = {'POPEST2019_CIV': '65PlusPop'})
    total_pop = census_by_age[(census_by_age['SEX'] == 0)&(census_by_age['AGE']==999)][['NAME','POPEST2019_CIV']].rename(columns = {'POPEST2019_CIV': 'totalPop'})
    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['NAME'],
                                                how='outer'), [plus12, plus18, plus65, total_pop])
    df_merged.rename(columns={'NAME':'State'}, inplace = True)
    state_pops = df_merged.merge(state_abbv, on='State')[['Code', '12PlusPop', '18PlusPop', '65PlusPop', 'totalPop']]
    state_pops.rename(columns={'Code':'Recip_State'}, inplace=True)
    
    #Obtains official vaccination data from the CDC website. 
    vacc = pd.read_csv('https://data.cdc.gov/api/views/8xkx-amqh/rows.csv?accessType=DOWNLOAD')
    vacc['Date'] = [datetime.strptime(x, '%m/%d/%Y') for x in vacc['Date']]
    #Since vaccination data is by county, sums all county together to create state totals. 
    vacc_state_totals = vacc.groupby(['Date','Recip_State'])['Series_Complete_Yes', 'Series_Complete_12Plus', 'Series_Complete_18Plus', 
                                                      'Series_Complete_65Plus', 'Administered_Dose1_Recip', 
                                                        'Administered_Dose1_Recip_12Plus', 'Administered_Dose1_Recip_18Plus', 
                                                      'Administered_Dose1_Recip_65Plus'].sum()
    vacc_state_totals.reset_index(inplace=True)
    #Merges state vaccination data with state census data to create percentages of the vaccinated population. 
    vacc_state_totals_abbv = vacc_state_totals.merge(state_pops, on='Recip_State')
    vacc_state_totals_abbv['Series_Complete_Pop_Pct'] = vacc_state_totals_abbv['Series_Complete_Yes']/vacc_state_totals_abbv['totalPop']*100
    vacc_state_totals_abbv['Series_Complete_12PlusPop_Pct'] = vacc_state_totals_abbv['Series_Complete_12Plus']/vacc_state_totals_abbv['12PlusPop']*100
    vacc_state_totals_abbv['Series_Complete_18PlusPop_Pct'] = vacc_state_totals_abbv['Series_Complete_18Plus']/vacc_state_totals_abbv['18PlusPop']*100
    vacc_state_totals_abbv['Series_Complete_65PlusPop_Pct'] = vacc_state_totals_abbv['Series_Complete_65Plus']/vacc_state_totals_abbv['65PlusPop']*100

    correct_order = ['Date', 'Recip_State', 'Recip_County', 'Series_Complete_Yes', 'totalPop', 'Series_Complete_Pop_Pct',
                 'Series_Complete_12Plus', '12PlusPop', 'Series_Complete_12PlusPop_Pct', 'Series_Complete_18Plus',
                 '18PlusPop', 'Series_Complete_18PlusPop_Pct', 'Series_Complete_65Plus', '65PlusPop', 
                'Series_Complete_65PlusPop_Pct']
    
    vacc_state_totals_abbv['Administered_Dose1_Pop_Pct'] = vacc_state_totals_abbv['Administered_Dose1_Recip']/vacc_state_totals_abbv['totalPop']*100
    vacc_state_totals_abbv['Administered_Dose1_Recip_12PlusPop_Pct'] = vacc_state_totals_abbv['Administered_Dose1_Recip_12Plus']/vacc_state_totals_abbv['12PlusPop']*100
    vacc_state_totals_abbv['Administered_Dose1_Recip_18PlusPop_Pct'] = vacc_state_totals_abbv['Administered_Dose1_Recip_18Plus']/vacc_state_totals_abbv['18PlusPop']*100
    vacc_state_totals_abbv['Administered_Dose1_Recip_65PlusPop_Pct'] = vacc_state_totals_abbv['Administered_Dose1_Recip_65Plus']/vacc_state_totals_abbv['65PlusPop']*100
    vacc_state_totals_abbv['Recip_County'] = 'Total'
    vacc_state_totals_abbv = vacc_state_totals_abbv[correct_order]
    vacc_state_totals_abbv.rename(columns={'Recip_State':'state', 'Date': 'date'}, inplace=True)
    
    #Creates master dataframe by merging the case/death, hospitalization, testing, and vaccination data together. 
    cases_hosp_test_vacc = reduce(lambda  left,right: pd.merge(left,right,on=['state', 'date'],
                                        how='outer'), [cases, hosp, test, vacc])
    cols_list = cases_hosp_test_vacc.columns.tolist()
    cols_list[0] = cols_list[14]
    cols_list.insert(1, 'state')
    cols_list.remove('Recip_County')
    del cols_list[15]
    cases_hosp_test_vacc = cases_hosp_test_vacc[cols_list].sort_values('date', ascending=False)
    return cases_hosp_test_vacc
