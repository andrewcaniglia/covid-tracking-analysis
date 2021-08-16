"""Pulls Covid data from four separate US Gov databases and emerges them
together to create a single data source"""
from datetime import datetime
from functools import reduce
import pandas as pd

HD_TEST = 'https://beta.healthdata.gov/api/views/j8mb-icvb/rows.csv?accessType=DOWNLOAD'
HD_HOSP = 'https://beta.healthdata.gov/api/views/g62h-syeh/rows.csv?accessType=DOWNLOAD'
CDC_CASES= 'https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD'

STATE_ABBV = 'https://worldpopulationreview.com/states/state-abbreviations'
CENSUS_AGE = 'https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/asrh/sc-est2019-agesex-civ.csv'
CDC_VACC = 'https://data.cdc.gov/api/views/8xkx-amqh/rows.csv?accessType=DOWNLOAD'

def get_cases():
    """Gathers case/death data"""
    cases = pd.read_csv(CDC_CASES)
    cases['date'] = [datetime.strptime(x, '%m/%d/%Y') for x in cases['submission_date']]
    cases.drop('submission_date', axis=1, inplace=True)
    cases['cfr'] = cases['new_death']/cases['new_case']*100
    return cases

def get_hosp():
    """Gathers hospitalization data"""
    hosp = pd.read_csv(HD_HOSP).sort_values('date', ascending=False)
    hosp['date'] = [datetime.strptime(x, '%Y/%m/%d') for x in hosp['date']]
    return hosp

def get_test():
    """Gathers testing data"""
    test = pd.read_csv(HD_TEST).sort_values(by='date',ascending=False)
    test['date'] = [datetime.strptime(x, '%Y/%m/%d') for x in test['date']]
    test = test.pivot(index=['date', 'state'], columns = 'overall_outcome', values = [
      'new_results_reported', 'total_results_reported'])
    test.reset_index(inplace=True)
    return test

def get_state_pop():
    """Creates a table of US state populations"""
    state_abbv = pd.read_html(STATE_ABBV)[0]
    census_by_age = pd.read_csv(CENSUS_AGE).rename(columns={'NAME':'State'})
    census_by_age_all = census_by_age.loc[census_by_age['SEX']==0]
    state_pop = census_by_age_all.merge(state_abbv, on='State')
    state_pop.rename(columns={'Code':'state'}, inplace=True)
    return state_pop

def get_pop_cat():
    """Calculates the US state populations for persons 12+, 18+, and 65+
    in age"""
    ages = [12, 18, 65, 999]
    cat_list = []

    for val in ages:
        cat_list.append(get_state_pop().loc[(get_state_pop()['AGE']>=
            val)&(get_state_pop()['AGE']<val+100)].groupby('state').sum().reset_index()[
              ['state', 'POPEST2019_CIV']].rename(columns = {
                'POPEST2019_CIV': str(val) + 'PlusPop'}))
    pop_cat = reduce(lambda  left,right: pd.merge(left,right,on=['state'],
                                                how='outer'), cat_list)
    pop_cat.rename(columns={'999PlusPop':'totalPop'}, inplace = True)
    return pop_cat

def get_vacc():
    """gathers vaccine data and merges it with the US State population
    data by age group"""
    vacc = pd.read_csv(CDC_VACC).rename(columns={'Recip_State':'state'})
    vacc['date'] = [datetime.strptime(x, '%m/%d/%Y') for x in vacc['Date']]
    vacc_state_totals = vacc.groupby(['date','state'])[['Series_Complete_Yes',
                          'Series_Complete_12Plus', 'Series_Complete_18Plus',
                          'Series_Complete_65Plus', 'Administered_Dose1_Recip',
            'Administered_Dose1_Recip_12Plus', 'Administered_Dose1_Recip_18Plus',
            'Administered_Dose1_Recip_65Plus']].sum().reset_index()
    vacc_state_totals_abbv = vacc_state_totals.merge(get_pop_cat(), on='state')
    return vacc_state_totals_abbv

def get_vacc_pop_pct():
    """Caculates the percentage of each age group that is either
    partially or fully vaccinated and adds it to the vaccination data."""
    vacc_data = get_vacc()
    values = ['Series_Complete_12Plus',
              'Series_Complete_18Plus', 'Series_Complete_65Plus',
             'Administered_Dose1_Recip_12Plus', 'Administered_Dose1_Recip_18Plus',
              'Administered_Dose1_Recip_65Plus']
    for val in values:
        vacc_data.insert(len(vacc_data.columns), f'{val}_Pop_Pct',
            vacc_data[val]/vacc_data[f'{val.rsplit("_", maxsplit=1)[-1][0:2]}PlusPop']*100)
    vacc_data['Administered_Dose1_Pop_Pct'] = vacc_data[
        'Administered_Dose1_Recip']/vacc_data['totalPop']*100
    vacc_data['Series_Complete_Pop_Pct'] = vacc_data[
        'Series_Complete_Yes']/vacc_data['totalPop']*100
    return vacc_data

def get_all_data():
    """Combines all COVID data into a single source"""
    all_data = reduce(lambda  left,right: pd.merge(left,right,
                                        on=['state', 'date'], how='outer'), [
                                          get_cases(), get_hosp(), get_test(), get_vacc_pop_pct()])
    all_data.sort_values('date', ascending = False, inplace=True)
    all_data.reset_index(drop=True, inplace=True)
    columns_order = list(all_data.columns)
    columns_order.insert(0, 'date')
    columns_order.pop(15)
    all_data = all_data[columns_order]
    return all_data

usa_covid_data = get_all_data()
