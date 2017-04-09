from base_type import base_type_int, base_type_float, base_type_datetime
from utils import *
from pyspark.sql import SparkSession
import sys
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
    data, rows = read_data(spark=spark, file=sys.argv[1])

    # Separate columns that are in :data from those that are not.
    valid_columns = validate_request(user_columns=sys.argv[2:], data_columns=data.columns)

    # Pretty-print information detailing number of rows for reference.
    row_description(row_count=rows)

    # Analyze valid columns only, printing out time diagnostics along the way.
    for index, column in enumerate(valid_columns):

        iter_start = time()

        print(
            '-' * 50,
            "Analyzing column: {} ({}/{})".format(column, index + 1, len(valid_columns)),
            '-' * 50,
            sep='\n'
        )

        analyze(column, data, rows)

        print('\nTook {:.1f} seconds.\n'.format(time() - iter_start))

    # Obtain measure of total time elapsed.
    print("Total time: {:.1f} seconds".format(time() - global_start))


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

    # TODO get rows of largest
    # report_semantic_type(semantic_type_results, rows)

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
            .map(lambda pair: (len(pair[1]), len(set(pair[1])), sample(set(pair[1]), min(100, len(set(pair[1]))))))
            .collect()
    )

    return result_dict


def report_base_type(base_type_dict, nrows):
    """Reporter function for current column's base type.

    :param base_type_dict: dictionary mapping {function : (amnt, uniq.amnt, vals)}.
    :param nrows: used for percentage reporting; total number of observations in this data.
    """

    # Iteration containers for building our lines and storing values.
    look_aside = enumerate(potential_base_types + [str])
    lines, values = [], []

    # For each base type in our complete list of base types
    for index, base_type in look_aside:

        try:
            # Attempt to isolate the raw number of values in current column
            # that were cast to :base_type.
            amount = base_type_dict[base_type][0][0]

            # Attempt to isolate the sample set of values that contain values
            # that were cast to :base_type.
            sample_values = base_type_dict[base_type][0][2]

        except IndexError:
            # If either don't exist, set both to representative 'empty' values.
            amount = 0
            sample_values = []

        # :lines will store what to print on each line, so store:
        #   1) the string-friendly representation of our function
        #   2) the raw number of casted values in our column
        #   3) the percentage of values in the entire column that were cast to this type
        lines.append([potential_base_type_names[index], amount, (amount / nrows) * 100])

        # :values will store sample values for each type, which we will pretty-print.
        values.append(sample_values)

    # We define our percentage width now -- this sets :width to 5 if any of the raw counts
    # represented by :amount is equal to nrows (that is, if any type for this column was
    # determined to be the type for the entire column) and otherwise sets :width to 4.
    width = 5 if nrows in [line[1] for line in lines] else 4

    # For each line representing a base function type and the set of values
    # representing that base type, print that frequency distribution to the user.
    for (line, line_values) in zip(lines, values):

        # Print top-level line denoting (1) type, (2) raw count, (3) proportion.
        print('|- {0:8s} => {1:10,d} = {2:>0{width}.1f}%'.format(line[0], line[1], line[2], width=width))

        # If we have any sample values to present, pretty-print them.
        if line[1] > 0:
            pretty_print_matrix(line_values, indent='\t', per_row=2)


if __name__ == '__main__':

    # Provide information on what this script can do.
    #   $ spark-submit faq.py
    if len(sys.argv) == 1:
        cli_help()

    # Provide information on what columns are available for analysis.
    #   $ spark-submit faq.py <input_file>.csv
    elif len(sys.argv) == 2:
        header, filename = get_header(sys.argv[1])
        print_prompt(header, filename)

    # Perform analysis on desired columns (or all columns, if :all passed)
    #   $ spark-submit faq.py <input_file>.csv :all
    #   $ spark-submit faq.py <input_file>.csv 'column 1' ... 'column n'
    else:
        main()
