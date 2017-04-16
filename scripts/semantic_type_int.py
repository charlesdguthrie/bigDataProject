'''
semantic_type_int.py

Check semantic type of ints
'''
import os
from functools import partial


def _check_zip_validity(value, nys_zips):
    '''
    validate zip codes, making sure they are within New York State
    '''
    try:
        value = int(value)
    except:
        return (None, None)

    if value in nys_zips:
        return ('zip', 'valid')

    elif (value>0) and (value<=99999):
        return ('zip', 'invalid')

    else:
        return (None, None)

with open('nys_zips.txt','r') as f:
    zipf = f.read()
nys_zips = zipf.split(os.linesep)[1:]

check_zip_validity = partial(_check_zip_validity, nys_zips=nys_zips)

int_checks = [check_zip_validity]