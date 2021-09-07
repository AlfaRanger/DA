## This poc script for converting WIC R code to python
your_project_id = "project-wic_poc"
import pandas as pd
from google.cloud import bigquery
import sys
import time

## global variables
projectId = 'chmdev'
dbName = 'DATASETCHM2021_D1'
tableNamePrefix = ''
moduleName = ''
year = ''
dbConn = bigquery.Client() ##global conn

## decorator
def add_datetime(func):
    reVal = ''

    def wrapper():
        start = time.perf_counter()
        print(func.__name__, " : method started")
        func()
        print(func.__name__, " : method stopped")

    return reVal


## create db connection
def get_db_conn():
    return bigquery.Client()


## get the query to run
def get_query(tableNamePrefix):
    tableName = tableNamePrefix + '_' + year
    baseQuery = "SELECT * FROM `" + projectId + "." + dbName + "." + tableName + "`"
    return baseQuery


## get specific query
# @add_datetime
def specificQuery(cat):
    queryWhole = """SELECT a.*, a.Family_zip as ZipCode, case when a.certification_category in (1,2,3) then b.MomPopulation
    else b.ChildPopulation end as PopEstimates FROM `chmdev.DATASETCHM2021_D1.MD_WIC_2019` a
    left join `chmdev.DATASETCHM2021_D1.MD_PopEstimates2019` b on a.Family_zip= b.ZipCode"""

    ### WIC data by catorgery
    ###Mom data
    queryMom = """SELECT a.*, a.Family_zip as ZipCode, case when a.certification_category in (1,2,3) then b.MomPopulation
    else b.ChildPopulation end as PopEstimates FROM `chmdev.DATASETCHM2021_D1.MD_WIC_2019` a 
    left join `chmdev.DATASETCHM2021_D1.MD_PopEstimates2019` b on a.Family_zip = b.ZipCode
    where a.certification_category in (1, 2, 3)
    """

    ### Child data
    queryChild = """SELECT a.*, a.Family_zip as ZipCode, case when a.certification_category in (1,2,3) then b.MomPopulation
    else b.ChildPopulation end as PopEstimates FROM `chmdev.DATASETCHM2021_D1.MD_WIC_2019` a left
    join `chmdev.DATASETCHM2021_D1.MD_PopEstimates2019` b on a.Family_zip = b.ZipCode 
    where a.certification_category in (5)
    """
    ### Infant data
    queryInfant = """SELECT a.*, a.Family_zip as ZipCode, case when a.certification_category in (1,2,3) then b.MomPopulation
        else b.ChildPopulation end as PopEstimates FROM `chmdev.DATASETCHM2021_D1.MD_WIC_2019` a left
        join `chmdev.DATASETCHM2021_D1.MD_PopEstimates2019` b on a.Family_zip = b.ZipCode 
        where a.certification_category in (4)"""

    ## National Risk Factor data
    queryNRF = """SELECT  *  FROM `chmdev.DATASETCHM2021_D1.WIC_RiskFactors`"""

    ## assigning appropriate query
    if(cat == 'all'):
        query = queryWhole
    elif(cat == 'mom'):
        query = queryMom
    elif(cat == 'child'):
        query = queryChild
    elif(cat == 'infant'):
        query = queryInfant
    elif(cat == 'nrf'):
        query = queryNRF

    return query


def run_SQL(dbConn, queryString):
    return dbConn.query(queryString).to_dataframe()

## get indicators
def get_indicators():
    global dbConn
    queryInd = """select case when VarCode='Currently.BF' then 'Currently_BF' when VarCode='Migrant.Status' then 'Migrant_Status'
    when VarCode='Ever.BF' then 'Ever_BF' else Varcode end as Ind 
    from (select distinct varcode  from `chmdev.DATASETCHM2021_D1.WIC_Codelookup`
    where Dataset= 'WIC' and VarType= 'Indicator' and Varcode 
    not in ('FamilyIncome', 'Nutritional.Risk.check', 'Income.Period', 'NRFactor') order by Varcode asc )"""

    dfInd = run_SQL(dbConn,queryInd)
    return dfInd

## get dimensions
def get_dimensions():
    global dbConn
    queryDim = """select distinct Dim from (select  case 
    when Varcode in ('AgeRangeMoms', 'AgeRangeChild', 'AgeRangeInfant' ) then 'AgeRange' 
    else Varcode end as Dim  from `chmdev.DATASETCHM2021_D1.WIC_Codelookup` 
    where Dataset= 'WIC' and VarType= 'Dimension' and Varcode not in ('NRFactor'))"""

    dfDim = run_SQL(dbConn,queryDim)
    return dfDim

## get population estimates
def get_pop():
    global dbConn
    ##[['ZipCode','ChildPopulation','MomPopulation']]
    queryPop = """ select * from `chmdev.DATASETCHM2021_D1.MD_PopEstimates2019`"""
    dfPop = run_SQL(dbConn, queryPop)
    dfPop['PopEstimates'] = dfPop['ChildPopulation'] + dfPop['MomPopulation']
    return dfPop

def get_riskf():
    global dbConn
    queryRisk = """ SELECT distinct RF_TYPE_RISK_FACTOR_TYPE_ID as col1  
    FROM `chmdev.DATASETCHM2021_D1.WIC_RiskFactors` where HIGH_RISK_FLAG=1 """

    dfRisk = run_SQL(dbConn,queryRisk)
    return dfRisk

def get_risk_factors(dfRisk):
    riskList = dfRisk.iloc[:,0].tolist()
    return riskList


def get_risk_counts(dfWICRisk):
    dfRiskMelt = pd.melt(dfWICRisk, id_vars="Family_zip")

    # dfRiskMelt.columns[dfRiskMelt.columns != 'Family_zip'].to_list()
    # kind of gather ***** check later
    ##dfCrossTabRisk = pd.crosstab(index=dfRiskMelt['Family_zip'], columns=dfRiskMelt.columns[dfRiskMelt.columns != 'Family_zip'].to_list())

    # dfRiskMelt[1:10]

    # dfRiskSpreadOut = pd.crosstab(index=[dfRiskMelt['Family_zip'],dfRiskMelt['variable']], columns=dfRiskMelt['value'])
    dfRiskSpreadOut = pd.crosstab(index=dfRiskMelt['Family_zip'], columns=dfRiskMelt['value'])
    dfRiskSpreadOut = dfRiskSpreadOut.reset_index()
    dfRiskZipCountMelt = pd.melt(dfRiskSpreadOut, id_vars=["Family_zip"], var_name='RiskID', value_name='Count')

    ## Do not delete these 2 comments
    dfRiskZipCountMelt = dfRiskZipCountMelt.sort_values(by=['Count'], ascending=False)
    # dfRiskZipCountMelt['Count_Denom'].sum()

    return dfRiskZipCountMelt

## get totals for that zip in the data in WIC data
def get_zip_counts(dfWIC):
    dfWICZip = dfWIC[['Case_ID', 'Family_zip']]
    dfWICZipCounts = dfWICZip.groupby('Family_zip')['Family_zip'].count().reset_index(name='Zip_Counts')
    return dfWICZipCounts

## get age unadjusted rates
def get_unadjusted(dfWICNRF, dfRiskCount, dfZipCounts):
    dfTemp1 = dfRiskCount.merge(dfWICNRF, left_on='RiskID',right_on='RF_TYPE_RISK_FACTOR_TYPE_ID')
    dfTemp2 = dfTemp1.merge(dfZipCounts, left_on='Family_zip',right_on='Family_zip')
    print(dfTemp2.columns)
    dfFinal = dfTemp2[['Family_zip','Count', 'CrossWalk','Zip_Counts']]
    dfFinal['Percentage'] = dfFinal['Count']/dfFinal['Zip_Counts']

    return dfFinal.drop_duplicates()

## get age/population adjusted rates
def get_adjusted(dfWICNRF, dfRiskCount, dfPop):

    dfTemp1 = dfRiskCount.merge(dfWICNRF, left_on='RiskID', right_on='RF_TYPE_RISK_FACTOR_TYPE_ID')
    dfTemp2 = dfTemp1.merge(dfPop, left_on='Family_zip', right_on='ZipCode')
    print(dfTemp2.columns)
    dfFinal = dfTemp2[['Family_zip', 'Count', 'CrossWalk', 'PopEstimates', '']]
    dfFinal['Percentage'] = dfFinal['Count'] / dfFinal['PopEstimates']

    return dfFinal.drop_duplicates()


## run the Stratification by Risk factors
def run_strat_rf():
    ## WIC whole
    query = specificQuery('all')
    dfWIC = run_SQL(dbConn, query)

    ##WIC NRF
    query = specificQuery('nrf')
    dfWICNRF = run_SQL(dbConn, query)

    ## getting the risk factors
    riskList = ['risk_1', 'risk_2', 'risk_3', 'risk_4', 'risk_5', 'risk_6', 'risk_7',
                'risk_8', 'risk_9', 'risk_10', 'Family_zip']
    dfWICRisk = dfWIC[riskList]
    dfRiskCount = get_risk_counts(dfWICRisk)
    dfZipCounts = get_zip_counts(dfWIC)
    print(dfRiskCount.head())
    print(dfZipCounts.head())
    # m = ZIP counts
    # df = dfRisk counts
    # WIC_NRF
    dfUnadj = get_unadjusted(dfWICNRF, dfRiskCount, dfZipCounts)
    print(dfUnadj.head)
    dfAdj = get_adjusted(dfWICNRF, dfRiskCount, get_pop())
    print(dfAdj.head())

def run_wic_state_au():

    pass


def main():
    ## Steps
    """
    1. read the data from db
    2. read the codelook ups
    3. group/slice the data for respective sections
    4. perform analysis - current version has 3 functions
    5. ? add metadata
    6. ? combine the results.
    :return:
    """
    ## 1. function to run stratification by risk factor
    run_strat_rf()

    ## 2. function to run functions for combinations



## main function
if (__name__ == '__main__'):
    print("Script initiated")
    main()
    print("Script ended")






