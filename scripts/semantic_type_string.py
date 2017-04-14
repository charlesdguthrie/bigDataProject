'''
semantic_type_string.py

Check semantic type of strings
'''

def check_borough_validity(value):
    '''
    check to see if value is among the 5 boroughs.
    works for:
        borough
        park borough
        taxi co. borough
    '''
    valid_boroughs = ['BRONX','BROOKLYN','MANHATTAN','QUEENS','STATEN ISLAND']
    if str.upper(value) in valid_boroughs:
        return ('borough','valid')
    else:
        return (None,None)


string_checks = [check_borough_validity]