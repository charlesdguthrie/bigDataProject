
def base_type_check(val, cast):
    """All-in-one function to determine valid value castings.

    This function takes a value :val and a casting function :cast
    and attempts to cast :val as a value of type :cast. We do this
    to determine potential base type values.

    :param val: any value of any type
    :param cast: cast function to be applied to :val
    :return (Bool, val): tuple denoting whether the value was correctly
        able to be cast to type :cast and the original value
    """
    try:
        attempt = cast(val)

        # Handle float case: 47.3 != int(47.3).
        if cast is float:

            if val is attempt or val == attempt:
                return (True, val)
            else:
                return (False, val)

        else:
            return (True, val)

    except:
        return (False, val)