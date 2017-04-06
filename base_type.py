
def base_type_check(val, cast):
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