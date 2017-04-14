from sys import exit


def cli_help():
    """Provide assistance if the user signals they need help.

    This function is only called if the user runs this script as
    `spark-submit faq.py` with no input file. If so, we inform the user
    on what actions they can take to proceed with the analysis and exit.
    """

    print(
        "\n-----------------------------------------------",
        "This file requires certain command-line inputs:",
        "-----------------------------------------------\n",

        "To get a pretty printing of your data's columns, make sure you pass in",
        "the data (with a header line containing column names, separated by commas) like so:",

        "\n\t$ spark-submit faq.py <file>.csv\n",

        "We suggest running the above if you're not sure exactly what column",
        "you'd like to analyze. Once you know what column you're looking to",
        "explore, run the following to actually analyze:",

        "\n\t$ spark-submit faq.py <file>.csv '<column name case-sensitive>'\n",

        "Note: you can also submit multiple columns to be evaluated within one run:",

        "\n\t$ spark-submit faq.py <file>.csv '<column 1>' '<column 2>' ... '<column n>'\n",
        sep='\n')


def read_data(spark, file):
    """Reads input CSV and returns Spark DataFrame object

    :return data: Spark DataFrame representing user-provided CSV
    """

    data = spark.read.csv(path=file, header=True)
    return data


def validate_request(user_columns, data_columns):
    """Determine what user-provided columns are valid and which are invalid.

    This function takes the user command-line inputs, puts them into a set,
    and comparing that set to the set of valid columns in the data. If the user
    input any invalid columns (e.g. 'Lstitude' instead of 'Latitude'), inform
    the user. We only analyze columns that are both user-specified and valid.

    :param user_columns: columns requested for analysis by user
    :param data_columns: columns of the Spark DataFrame object
    :return valid: set of column names as strings
    """

    # Isolate our user- and data-columns into sets.
    data_columns_set = set(data_columns)
    user_columns_set = set(user_columns)

    # If the user denotes :all keyword, analyze all columns.
    if ':all' in user_columns_set:
        return data_columns

    # Valid columns are in the intersection between the two,
    # invalid columns are in the difference from user to data columns.
    valid, invalid = (
        user_columns_set.intersection(data_columns_set),
        user_columns_set.difference(data_columns_set)
    )

    # For all invalid columns, inform the user of their invalidity.
    for column in invalid:
        print("`{}` is not a valid column --- skipping.".format(column))

    # Proceed with the analysis using only valid columns.
    return valid


def row_description(row_count):
    """Inform user of data's row count at start of script.

    :param row_count: number of rows in data -- computed with data.count()
    """
    line = 'Distribution results based on {:,} rows'.format(row_count)
    print(
        '+', '-' * (len(line) + 2), '+\n',
        '| ', line, ' |\n',
        '+', '-' * (len(line) + 2), '+\n',
        sep=''
    )


def pretty_print_matrix(values_list, indent='', per_row=2):
    """Pretty print a list of values.

    The following code for pretty-printing a matrix was sourced from:
        http://stackoverflow.com/questions/13214809/pretty-print-2d-python-list
    We utilize this code to better display the available columns to the user,
    and does not influence our project's findings or hypotheses.

    :param values_list: one-dimensional list of values to be printed (nicely)
    :param indent: indent to be applied to each line -- usually '\t', but defaults ''
    :param per_row: requested number of values per row
    """

    # Convert our list to a matrix (list of lists) to work properly.
    matrix = [values_list[i:i + per_row] for i in range(0, len(values_list), per_row)]

    # Do...the thing.
    lens = [max(map(safe_len, col)) for col in zip(*matrix)]
    fmt = indent + '\t'.join('{{:{}}}'.format(x) for x in lens)
    table = [fmt.format(*[str(i) for i in row]) for row in matrix]
    print('\n'.join(table))


def safe_len(val):
    """Perform a safe evaluation of a value's length.

    We define :safe_len so that :pretty_print_matrix doesn't get tripped-up
    when it encounters a `None` value during map(len, col). :safe_len is a thin
    wrapper around the builtin :len that has any exceptions -- namely, len(None) --
    return 4 (len(str(None))) instead of raising a TypeError.

    :param val: value whose length we'd like to compute
    :return: the length of the value
    """

    try:
        return len(val)

    # Only applicable for `None` type, so return len(str(None)) (= 4).
    except:
        return 4


def print_prompt(column_names, filename):
    """Pretty-print column names to the user.

    This function is only called if the user runs this script as
    `spark-submit faq.py <input_file>.csv` and does not specify columns.
     We take that to mean that they want a better understanding of what
     columns they *can* choose from, so given we already read the first line
     (see get_header()), pretty-print those column names.

    :param column_names: list of strings representing column names from input CSV
    :param filename: name of input file as given by user
    """

    # Define our prompt and print the header to our 'table'.
    prompt = "\nInput file contained the following columns:"
    print(prompt, '-' * len(prompt), sep='\n')

    # The following code for pretty-printing a matrix was sourced from:
    #   http://stackoverflow.com/questions/13214809/pretty-print-2d-python-list
    # We utilize this code to better display the available columns to the user,
    # and does not influence our project's findings or hypotheses.
    pretty_print_matrix(column_names, indent='\t', per_row=4)

    # Inform the user of how they can proceed.
    print(
        "\nHence, you can now run something like:\n",
        "\t$ spark-submit faq.py {0} \'{1}\' \n".format(filename, column_names[0]),
        "or, to run analysis on all columns:\n",
        "\t$ spark-submit faq.py {0} :all\n".format(filename),
        sep="\n"
    )


def get_header(file):
    """Obtain the first line of a file.

    This function is primarily reserved for when a user does not specify
    columns and only runs `spark-submit faq.py <input file>.csv`. We take
    that to mean they want a nice output of what columns they *can* choose
    from, so just read the first line (which contains the column names).

    :return first_line: list of strings representing first line of comma-delimited file
    """

    # Attempt to read just the first line, split it, and return it.
    try:
        with open(file, 'r') as f:
            first_line = [name.strip() for name in f.readline().split(',')]
            return first_line, file

    # Handle auxiliary exceptions.
    except FileNotFoundError:
        print("Error: could not find file `{}`. Please try again.".format(file))
        print("Exiting...")
        exit(0)

    except PermissionError:
        print("Error: you do not have read-permission on file `{}`. Please try again.".format(file))
        print("Exiting...")
        exit(0)