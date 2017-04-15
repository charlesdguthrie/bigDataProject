'''
semantic_type_date.py

Check date semantic types
'''
from functools import partial
from datetime import datetime


def _check_date_validity(value, mindate, maxdate, fmt):
    """
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
        tuple ('date','valid'),('date','invalid'), or (None,None)
    """
    
    # first check if it is a datetime, using base_type_results
    try:
        check_date = datetime.strptime(value,fmt)

    except:
        return (None, None)
        
    # next, check if value is between 1/1/2009 and 1/1/2017
    if (check_date >= mindate) and (check_date <= maxdate):
        return ('date', 'valid')

    else:
        return ('date', 'valid')


check_date_validity = partial(_check_date_validity,
                              mindate=datetime(2009, 1, 1, 0, 0),
                              maxdate=datetime(2017, 1, 1, 0, 0),
                              fmt='%m/%d/%Y %I:%M:%S %p')

date_checks = [check_date_validity]
