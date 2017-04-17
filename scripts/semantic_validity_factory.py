
def semantic_validity_factory(value, semantic_name, null_values=set(['Unspecified', 'N/A', '', 0, None]), **kwargs) :

    if value in null_values:
        return (None, None)

    elif all(f(value) for name, f in kwargs.items() if 'semantic' in name):
        if all(f(value) for name, f in kwargs.items() if 'valid' in name):
            return (semantic_name, 'valid')
        else:
            return (semantic_name, 'invalid')

    else:
        # only checking for semantic type!
        return (None, None)