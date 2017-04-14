from time import strptime
from functools import partial


def base_type_int(val):
    """Determine whether a value's base type is integer.

    :param val: value contained within our data

    :return:
        (Bool, val) tuple containing whether the value
        was correctly cast (or not) and the original value.
    """
    try:
        _ = int(val)
        return (True, val)

    except:
        return (False, val)


def base_type_float(val):
    """Determine whether a value's base type is float.

    :param val: value contained within our data

    :return:
        (Bool, val) tuple containing whether the value
        was correctly cast (or not) and the original value.
    """
    try:
        _ = float(val)
        return (True, val)

    except:
        return (False, val)


def _base_type_datetime(val, fmt):
    """Determine whether a value's base type is datetime.

    :param val: value contained within our data

    :return:
        (Bool, val) tuple containing whether the value
        was correctly cast (or not) and the original value.
    """

    if val is None:
        return (False, None)

    try:
        _ = strptime(val, fmt)
        return (True, val)

    except:
        return (False, val)


# Use a function partial to avoid passing in the date format
# each time we call _base_type_datetime. This is a bit inflexible
# in terms of the datetime formats we can parse, but works for our
# dataset just fine. A more robust solution would be to use
# dateutil.parser.parse(), but it's very slow.
base_type_datetime = partial(_base_type_datetime, fmt='%m/%d/%Y %I:%M:%S %p')
