'''
semantic_type_string.py

Check semantic type of strings
'''

def check_borough_validity(value):
    '''
    check to see if value is among the 5 boroughs.
    '''
    valid_boroughs = ['BRONX','BROOKLYN','MANHATTAN','QUEENS','STATEN ISLAND']
    if upper(value) in valid_boroughs:
        validity='valid'
    elif upper(value)=='UNSPECIFIED':
        validity='invalid'
    return ('borough',validity)


string_checks = [check_borough_validity]