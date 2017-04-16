#import re
#from functools import partial

def _semantic_validity_factory(value, semantic_name, null_values=set(('Unspecified','N/A','')), **kwargs):
    if value in null_values:
        return (None, None)
    elif all(f(value) for name, f in kwargs.items() if 'semantic' in name):
        if all(f(value) for name, f in kwargs.items() if 'valid' in name):
            return (semantic_name,'VALID')
        else:
            return (semantic_name,'INVALID')
    else:
        # only checking for semantic type!
        return (None, None)