'''
semantic_type_int.py

Check semantic type of ints
'''
import os
import re
from functools import partial
from semantic_validity_factory import _semantic_validity_factory,tester


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

#load New York State Zips
with open('scripts/nys_zips.txt','r') as f:
    zipf = f.read()

#skip first and last line
nys_zips = zipf.split(os.linesep)[1:-1]
#convert to int
nys_zips = [int(z) for z in nys_zips]

check_zip_validity = partial(_check_zip_validity, nys_zips=nys_zips)


# semantic_type = 'phone_num' (index: )
phone_num_valid = re.compile(r'(212|718|917)')
phone_num_semantic = re.compile(r'^[1-9]\d{9}$')
phone_num_args = {'semantic_match': phone_num_semantic.match,
                   'valid_check': phone_num_valid.match}

is_phone_number = partial(_semantic_validity_factory, semantic_name='phone_num', **phone_num_args)

int_checks = [check_zip_validity, is_phone_number]


if __name__ == '__main__':
    #Some unit testing
    tester(int_checks,test_type='all')