from base_type import base_type_int, base_type_float, base_type_datetime
from pyspark.sql import SparkSession
import sys
from pprint import pprint
from time import time
from random import sample


# Establish Spark session details.
spark = SparkSession.builder \
    .master("local") \
    .appName("311 Analysis") \
    .getOrCreate()

# Establish base function types.
potential_base_types = [base_type_int, base_type_float, base_type_datetime]
potential_base_type_names = ['int', 'float', 'datetime', 'string']


def main():

    # Obtain measure of total time elapsed.
    global_start = time()

    # Read input data.
    data, rows = read_data()

    # Separate columns that are in :data from those that are not.
    valid_columns = validate_request(data.columns)

    row_description(rows)

    # TODO consider re-writing to rely upon Spark, not Python.
    # TODO remove timing stuff for final submission.
    # Analyze valid columns only, printing out time diagnostics along the way.
    for index, column in enumerate(valid_columns):
        iter_start = time()
        print('-' * 50, "Analyzing column: {} ({}/{})".format(column, index + 1, len(valid_columns)), '-' * 50, sep='\n')
        analyze(column, data, rows)
        print('\nTook {:.1f} seconds.\n'.format(time() - iter_start))

    # Obtain measure of total time elapsed.
    print("Total time: {:.1f} seconds".format(time() - global_start))


def read_data():
    """Reads input CSV and returns Spark DataFrame object

    :return data: Spark DataFrame representing user-provided CSV
    """

    data = spark.read.csv(path=sys.argv[1], header=True)
    return data, data.count()


# TODO are column orders changing? What's going on?
def validate_request(data_columns):
    """Determine what user-provided columns are valid and which are invalid.

    This function takes the user command-line inputs, puts them into a set,
    and comparing that set to the set of valid columns in the data. If the user
    input any invalid columns (e.g. 'Lstitude' instead of 'Latitude'), inform
    the user. We only analyze columns that are both user-specified and valid.

    :param data_columns: columns of the Spark DataFrame object
    :return valid: set of column names as strings
    """

    # Isolate our user- and data-columns into sets.
    data_columns = set(data_columns)
    user_columns = set(sys.argv[2:])

    if ':all' in user_columns:
        return data_columns

    # Valid columns are in the intersection between the two,
    # invalid columns are in the difference from user to data columns.
    valid, invalid = user_columns.intersection(data_columns), user_columns.difference(data_columns)

    # For all invalid columns, inform the user of their invalidity.
    for column in invalid:
        print("`{}` is not a valid column --- skipping.".format(column))

    # Proceed with the analysis using only valid columns.
    return valid


def row_description(nrow):
    line = 'Distribution results based on {:,} rows'.format(nrow)
    print(
        '+', '-' * (len(line) + 2), '+\n',
        '| ', line, ' |\n',
        '+', '-' * (len(line) + 2), '+\n',
        sep=''
    )


# TODO semantic type checking -- Charlie/Dave
# TODO NULL/Invalid values (Problem Type 1) -- Charlie
# TODO Valid/Outlier values (Problem Type 2) -- Danny
def analyze(column, data, rows):
    """Perform common analyses for a given column in our DataFrame.

    :param column: string representing column within :data
    :param data: Spark DataFrame containing user-provided CSV values
    :param rows: number of rows contained in :data
    """

    # TODO consider Parquet columnar format?
    # Work with specific column as RDD.
    column = data.select(column).rdd

    # Check column against base types.
    base_type_results = analyze_base_type(column)

    report_base_type(base_type_results, rows)

    # Check column against semantic types.
    # semantic_type_results = analyze_semantic_type(column)

    # Compute column-wise aggregates.
    # TODO not sure if semantic_type_results should be utilized.
    # aggregate_results = analyze_aggregate(column, sample, semantic_type_results)


def analyze_base_type(data):
    """Perform initial base-type check of each column's values.

    This function performs a column-wise evaluation of the base-type distribution.
    Specifically, for each :base_type in :potential_base_types (the global
    list of base types), we attempt to cast that column'e values to :base_type.
    Once we do so, the origin data splits: send the valid casts to :valid
    and the invalid casts (the ones that didn't pass) to :remaining.

    :valid will be stored in the :result_dict dictionary under the 'int' key
    (the actual function, not the string), so we format it as the following
    tuple triplet:

        1. Number of non-unique values that were cast to :base_type
        2. Number of unique values that were cast to :base_type
        3. The set of values cast to :base_type (if the set's length < 100 -- for readability)

    We continue with :remaining as our new origin dataset. Once we've
    exhausted all :base_type in :potential_base_type, we attribute
    any remaining values as strings.

    Note: the sum of :result_dict[:base_type] for all :base_type in
    :potential_base_types should always equal the number of rows evaluated.

    What's nice about this implementation is that we reduce the work done
    with each potential base type -- by the time we get to str() we've
    gone through int(), float(), and datetime(), so integer-only columns
    take almost no time for the remaining type checks. A downside is that
    string-only columns will go through all castings.

    :param data: Spark DataFrame containing one column's values.

    :return
        result_dict: dictionary of tuples describing our column's
        valid base types.
    """

    # Initialize results container.
    result_dict = dict()

    # For each function in our list of potential base type check functions
    for base_type_function in potential_base_types:

        # Attempt to cast our column's values as one of base_type_function (int, float, datetime).
        # This returns an RDD :base_type_rdd of (Boolean, original_value) tuples that are True
        # if and only if that value was successfully cast to that type.
        base_type_rdd = data.map(lambda row: base_type_function(row[0])).cache()

        # Split our data into :valid and :remaining RDDs. As their name suggests,
        # :valid are rows that were correctly cast with :base_type_function, and
        # :remaining are rows that weren't.
        valid, remaining = (
            (
                # From rows of (True, val) tuples, we group by the only key (True)
                # and map the values to a list to preserve the originals. Next,
                # we map our (True, <list_of_values>) tuple to one that
                # looks like: (True, <length of list of values>, <length of set of values>).
                # (This is obviously flexible, and can be changed in the future. This
                # configuration tells us the number of values in the column that were
                # cast as :base_type_function and the number of unique values in that list.)
                base_type_rdd.filter(lambda pair: pair[0] is True)
                             .groupByKey()
                             .mapValues(list)
                             .map(lambda pair: (len(pair[1]), len(set(pair[1])), sample(set(pair[1]), min(100, len(set(pair[1]))))))
                             .collect()
            ),
            (
                # From rows of (False, val) tuples, we need to re-cycle them through
                # the casting functions. We thus have to make sure that when we access
                # row[0] on Line 157, that we access the correct value and not `False`.
                # So, we map our (False, val) pairs to (val, 0) in order to correctly
                # maintain our loop invariant.
                base_type_rdd.filter(lambda pair: pair[0] is False)
                             .map(lambda pair: (pair[1], 0))
            )
        )

        # Store the 3-length tuple into :result_dict under the key :base_type_function.
        result_dict[base_type_function] = valid

        # Assign our new dataset to our :remaining.
        data = remaining

    # When we're done, we're left with (val, 0) tuples that weren't able to be cast
    # as any of our defined base type functions. Hence, we default their casting to
    # string types since all types can be represented as strings. We don't do anything
    # new -- it's the same algorithm as we use for :valid.
    result_dict[str] = (
        data.map(lambda row: (True, row[0]))
            .groupByKey()
            .mapValues(list)
            .map(lambda pair: (len(pair[1]), len(set(pair[1])), sample(set(pair[1]), min(100,len(set(pair[1]))))))
            .collect()
    )

    return result_dict


def report_base_type(base_type_dict, nrows):

    look_aside = enumerate(potential_base_types + [str])
    lines = []

    for index, base_type in look_aside:

        try:
            amount = base_type_dict[base_type][0][0]
            sample_values = base_type_dict[base_type][0][2]

        except IndexError:
            amount = 0
            sample_values = []

        lines.append([potential_base_type_names[index], amount, (amount / nrows) * 100, sample_values])

    width = 5 if 100.0 in [line[2] for line in lines] else 4

    for line in lines:
        print('|- {0:8s} => {1:10,d} = {2:>0{width}.1f}%'.format(line[0], line[1], line[2], width=width))


def mod_len(val):
    try:
        return len(val)
    except:
        return 0


def get_header():
    """Obtain the first line of a file.

    This function is primarily reserved for when a user does not specify
    columns and only runs `spark-submit faq.py <input file>.csv`. We take
    that to mean they want a nice output of what columns they *can* choose
    from, so just read the first line (which contains the column names).

    :return first_line: list of strings representing first line of comma-delimited file
    """

    # Attempt to read just the first line, split it, and return it.
    try:
        with open(sys.argv[1], 'r') as f:
            first_line = [name.strip() for name in f.readline().split(',')]
            return first_line

    # Handle auxiliary exceptions.
    except FileNotFoundError:
        print("Error: could not find file `{}`. Please try again.".format(sys.argv[1]))
        print("Exiting...")
        sys.exit(0)

    except PermissionError:
        print("Error: you do not have read-permission on file `{}`. Please try again.".format(sys.argv[1]))
        print("Exiting...")
        sys.exit(0)


def print_prompt(column_names):
    """Pretty-print column names to the user.

    This function is only called if the user runs this script as
    `spark-submit faq.py <input_file>.csv` and does not specify columns.
     We take that to mean that they want a better understanding of what
     columns they *can* choose from, so given we already read the first line
     (see get_header()), pretty-print those column names.

    :param column_names: list of strings representing column names from input CSV
    """

    # Define our prompt and print the header to our 'table'.
    prompt = "\nInput file contained the following columns:"
    print(prompt, '-' * len(prompt), sep='\n')

    # The following code for pretty-printing a matrix was sourced from:
    #   http://stackoverflow.com/questions/13214809/pretty-print-2d-python-list
    # We utilize this code to better display the available columns to the user,
    # and does not influence our project's findings or hypotheses.

    # Convert our list to a matrix (list of lists) to work properly.
    names_matrix = [column_names[i:i + 4] for i in range(0, len(column_names), 4)]

    # Do...the thing.
    lens = [max(map(len, col)) for col in zip(*names_matrix)]
    fmt = '\t'.join('{{:{}}}'.format(x) for x in lens)
    table = [fmt.format(*row) for row in names_matrix]
    print('\n'.join(table))

    # Inform the user of how they can proceed.
    print(
        "\nHence, you can now run something like:\n",
        "\t$ spark-submit faq.py {0} \'{1}\' \n".format(sys.argv[1], column_names[0]),
        "or, to run analysis on all columns:\n",
        "\t$ spark-submit faq.py {0} :all\n".format(sys.argv[1]),
        sep="\n"
    )


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

    sys.exit(0)


if __name__ == '__main__':

    # Provide information on what this script can do.
    #   $ spark-submit faq.py
    if len(sys.argv) == 1:
        cli_help()

    # Provide information on what columns are available for analysis.
    #   $ spark-submit faq.py <input_file>.csv
    elif len(sys.argv) == 2:
        header = get_header()
        print_prompt(header)

    # Perform analysis on desired columns (or all columns, if :all passed)
    #   $ spark-submit faq.py <input_file>.csv :all
    #   $ spark-submit faq.py <input_file>.csv 'column 1' ... 'column n'
    else:
        main()
