'''
semantic_type_float.py

Check semantic type of floats
'''
from functools import partial

def _check_coordinate_validity(value,lat_min,lat_max,long_min,long_max):
    '''
    validate coordinate by making sure it is inside the box
    containing all of New York State
    '''

    try:
        value = float(value)
    except:
        return (None, None)
    
    if ((value >= lat_min) and (value <= lat_max)) or ((value >= long_min) and (value <= long_max)):
        return ('coordinate', 'valid')

    elif (value>=-180) and (value<=180):
        return ('coordinate', 'invalid')

    else:
        return (None, None)

check_coordinate_validity = partial(_check_coordinate_validity,
                                    lat_min=40.50, lat_max=45.01,
                                    long_min=-79.76, long_max=-71.86)


#After defining float check functions, add them to this list.  
float_checks = [check_coordinate_validity]

if __name__ == '__main__':
    #Some unit testing
    from semantic_validity_factory import tester
    tester(float_checks,test_type='all')