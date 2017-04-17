'''
semantic_type_string.py

Check semantic type of strings
'''
import re
from functools import partial
from .semantic_validity_factory import semantic_validity_factory


valid_boroughs = {'BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND'}


def check_borough_validity(value):
    '''
    check to see if value is among the 5 boroughs.
    works for:
        borough
        park borough
        taxi co. borough
    '''

    if type(value) != str:
        return (None, None)

    elif str.upper(value) in valid_boroughs:
        return ('borough', 'valid')

    else:
        return (None, None)


school_region_semantic = re.compile(r'(Alternative|Region|District)')
school_region_valid = re.compile(r'.+\s[1-9]$|.+\s10$|.+\s75$|.+\sSuperintendency$')
school_region_args = {'semantic_match': school_region_semantic.match,
                      'valid_check': school_region_valid.match}
is_school_region = partial(semantic_validity_factory, semantic_name='school_region', **school_region_args)


school_num_semantic = re.compile(r'([BMQRX]|\b)\d{1,3}(\b|[A-Z]\d{0,2}|\-\d{2,3}(\b|[A-Z]))')
school_num_args = {'semantic_match': school_num_semantic.match,
                   'valid_check': lambda x: len(x) <= 8}
is_school_number = partial(semantic_validity_factory, semantic_name='school_num', **school_num_args)


address_name_semantic = re.compile(r'([\w\,\.\-]+ *)+')
address_name_args = {'semantic_match': address_name_semantic.match,
                     'valid_check': lambda x: True}
is_address_name = partial(semantic_validity_factory, semantic_name='address_name', **address_name_args)


string_checks = [check_borough_validity, is_school_region, is_school_number, is_address_name]

