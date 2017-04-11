'''
semantic_type.py

Check semantic types
'''

def _check_date_validity(value, mindate, maxdate, fmt):
    '''
    validated a data,
    ensuring that all values are between 1/1/2009 and 1/1/2017
    
    args:
        value: date value to check
        mindate: minimum date allowed in the data
        maxdate: maximum date allowed in the data
        fmt: string format for date
    returns:
        tuple ('date',validity), where validity is
            one of 'null','valid','invalid'
    '''
    #for each value:
    
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