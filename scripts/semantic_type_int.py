'''
semantic_type_int.py

Check semantic type of ints
'''
from functools import partial
import re
from .semantic_validity_factory import semantic_validity_factory


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

    elif (value > 0) and (value <= 99999):
        return ('zip', 'invalid')

    else:
        return (None, None)

# Initialization required for checking zip code.
with open('scripts/nys_zips.txt', 'r') as f:
    zip_codes = f.readlines()
zip_codes = set([int(zip.strip('\n')) for zip in zip_codes])
check_zip_validity = partial(_check_zip_validity, nys_zips=zip_codes)

# Check for phone number semantic type.

# Define our validation and semantic parameters.
phone_num_valid = re.compile(r'(212|718|917)')
phone_num_semantic = re.compile(r'^[1-9]\d{9}$')
phone_num_args = {'semantic_match': phone_num_semantic.match,
                  'valid_check': phone_num_valid.match}

# Initialize partial function to check phone number.
is_phone_number = partial(semantic_validity_factory, semantic_name='phone_num', **phone_num_args)

# Define list of semantic type evaluation functions.
int_checks = [check_zip_validity, is_phone_number]

