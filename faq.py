from base_type import base_type_int, base_type_float, base_type_datetime
from utils import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import sys
import csv
import io
from time import time


# Establish Spark session and context.
spark = SparkSession.builder \
    .master("local") \
    .appName("311 Analysis") \
    .getOrCreate()
sc = SparkContext.getOrCreate()

# Establish base function types and names.
potential_base_types = [base_type_int, base_type_float, base_type_datetime]
base_type_names = {
    base_type_int: 'int',
    base_type_float: 'float',
    base_type_datetime: 'datetime',
    str: 'string'
}


def main():

    global_start = time()

    # Read input data.
    data = read_data(spark=spark, file=sys.argv[1])

    # Separate columns that are in :data from those that are not.
    valid_columns = validate_request(user_columns=sys.argv[2:], data_columns=data.columns)

    # Analyze valid columns only, printing out time diagnostics along the way.
    for index, column in enumerate(valid_columns):
        iter_start = time()
        analyze(column, data)
        print('\nColumn `{}` took {:.1f} seconds.\n'.format(column, time() - iter_start))

    print('\nTotal runtime: {} seconds.'.format(time() - global_start))


# TODO semantic type checking -- Charlie/Dave
# TODO NULL/Invalid values (Problem Type 1) -- Charlie
# TODO Valid/Outlier values (Problem Type 2) -- Danny
def analyze(column_name, data):
    """Perform common analyses for a given column in our DataFrame.

    This function encompasses the entire analysis performed on ONE column,
    including:

        1. base type evaluation
        2. semantic type evaluation
        3. valid/outlier value evaluation

    :param column_name: string representing column within :data
    :param data: Spark DataFrame containing user-provided CSV values
    """

    # Work with specific column as RDD.
    column_data = data.select(column_name).rdd

    # Check column against base types and report column base type evaluation.
    base_type_results = analyze_base_type(column_data)
    dominant_base_type = get_dominant_base_type(base_type_results)

    # Check column against semantic types.
    # semantic_type_results = analyze_semantic_type(base_type_results, dominant_base_type)

    # Compute column-wise aggregates.
    # aggregate_results = analyze_aggregate(column, sample, semantic_type_results)

    # Needs more!
    # merged_rdd = join_results(base_type_results)


def analyze_base_type(data):
    """Perform initial base-type check of each column's values.

    This function performs a column-wise evaluation of the base-type distribution.
    Specifically, for each :base_type in :potential_base_types (the global
    list of base types), we attempt to cast that column'e values to :base_type.
    Once we do so, the origin data splits: send the valid casts to :valid
    and the invalid casts (the ones that didn't pass) to :remaining.

    For example, on the first iteration :valid will store the valid values
    in :data that were cast to int. Then, we store those values (still as RDD)
    in the :result_dict dictionary under the 'int' key (the actual function,
    not the string).

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
        result_dict: dictionary of RDD's describing our column's valid base type values.
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
                # From rows of (True, val) tuples, extract the :val,
                # re-map to (val, <type val>) tuples.
                base_type_rdd.filter(lambda pair: pair[0] is True)
                             .map(lambda pair: (pair[1], base_type_names[base_type_function]))
                             .collect()
            ),
            (
                # From rows of (False, val) tuples, we need to re-cycle them through
                # the casting functions. We thus have to make sure that when we access
                # row[0] that we access the correct value and not `False`.
                # So, we map our (False, val) pairs to (val, 0) in order to correctly
                # maintain our loop invariant.
                base_type_rdd.filter(lambda pair: pair[0] is False)
                             .map(lambda pair: (pair[1], 0))
            )
        )

        # Store the 3-length tuple into :result_dict under the key :base_type_function.
        result_dict[base_type_function] = valid

        # Assign our origin dataset to our :remaining RDD.
        data = remaining

    # When we're done, we're left with (val, 0) tuples that weren't able to be cast
    # as any of our defined base type functions. Hence, we default their casting to
    # string types since all types can be represented as strings. We don't do anything
    # new -- it's the same algorithm as we use for :valid.
    result_dict[str] = data.map(lambda pair: (pair[0], base_type_names[str])).collect()

    return result_dict


def get_dominant_base_type(data_dictionary):
    """Obtain a simple representation of our column's dominant base type.

    :param data_dictionary: results dictionary returned by :analyze_base_type().
    :return dominant_type: this column's most prevalent base type.
    """

    # Define our key functions.
    all_functions = potential_base_types + [str]

    # Initialize our results container with defaults.
    dominant_type = {'type': None, 'length': 0}

    for base_type in all_functions:

        # Get the length of the number of values in column that
        # were cast as type :base_type().
        number_of_coercions = len(data_dictionary[base_type])

        # If that number is greater than our current max, store a nice
        # representation of the function name (representing type) and
        # a count of the number of values that were cast as such.
        if number_of_coercions > dominant_type['length']:
            dominant_type['type'] = base_type_names[base_type]
            dominant_type['length'] = number_of_coercions

    # Return our two-entry dictionary containing the most frequent base type.
    return dominant_type['type']


def join_results(data_dictionary):
    """Coalesce our fragmented RDD into one master RDD.

    :param data_dictionary: results dictionary returned by :analyze_base_type().
    :return master_rdd: Spark RDD containing all rows from dictionary.
    """

    # Define our list of key-functions.
    all_functions = potential_base_types + [str]

    # Initialize an empty RDD -- equivalent to creating an empty list.
    master_rdd = sc.emptyRDD()

    for base_type in all_functions:

        # Convert list of tuples into Spark RDD, then append that
        # RDD to our :master_rdd. This builds an RDD containing all of
        # the original column.
        rdd = sc.parallelize(data_dictionary[base_type])
        master_rdd = master_rdd.union(rdd)

    # Return our joined, complete column of tuples.
    return master_rdd


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
