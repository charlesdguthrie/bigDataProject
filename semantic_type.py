'''
semantic_type.py

Check semantic types
'''
import pandas as pd
def _check_date_validity(value, mindate, maxdate, fmt):
    '''
    validate dates,
    ensuring that all values are between 1/1/2009 and 1/1/2017
    works for:
        Created Date
        Closed Date TODO should be after created Date
        Due Date TODO should be after Created Date
        Resolution Action Updated Date TODO should be after Created Date
    
    args:
        value: date value to check
        mindate: minimum date allowed in the data
        maxdate: maximum date allowed in the data
        fmt: string format for date
    returns:
        tuple ('date',validity), where validity is
            one of 'null','valid','invalid'
    '''
    
    #first check if it is a datetime, using base_type_results
    try:
        check_date = datetime.strptime(value,fmt)
    except:
        validity='null'
        
    #next, check if value is between 1/1/2009 and 1/1/2017
    if (check_date>=mindate) and (check_date<=maxdate):
        validity='valid'
    else:
        validity='invalid'
    
    #validity = 'valid','invalid','null'
    return ('date',validity)

check_date_validity = partial(_check_date_validity, \
                              mindate = datetime(2009,1,1,0,0),\
                              maxdate = datetime(2017,1,1,0,0),\
                              fmt='%m/%d/%Y %I:%M:%S %p')

def _check_zip_validity(value,nys_zips):
    '''
    validate zip codes, making sure they are within New York State
    '''
    value = int(value)
    if value in nys_zips:
        validity='valid'
    else:
        validity='invalid'
    return (value,validity)

zipDF = pd.read_csv('data/nys_zips_clean.tsv',sep='\t')
nys_zips = list(zipDF['ZIP Code'])
check_zip_validity = partial(_check_zip_validity,\
    nys_zips=nys_zips)

def check_borough_validity(value):
    '''
    check to see if value is among the 5 boroughs.
    '''
    valid_boroughs = ['BRONX','BROOKLYN','MANHATTAN','QUEENS','STATEN ISLAND']
    if value in valid_boroughs:
        validity='valid'
    else:
        validity='invalid'
    return (value,validity)