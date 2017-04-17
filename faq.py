from scripts.base_type import base_type_int, base_type_float, base_type_datetime
from scripts.utils import *

from sys import argv
from time import time
from itertools import chain

from pyspark.sql import SparkSession
from pyspark.context import SparkContext


# Establish Spark session and context.
spark = SparkSession.builder \
                    .master("local") \
                    .appName("311 Analysis") \
                    .getOrCreate()

sc = SparkContext.getOrCreate()

# Establish base function types and names.
potential_base_types = [base_type_int, base_type_float, base_type_datetime]
base_type__names = {
    base_type_int: 'int',
    base_type_float: 'float',
    base_type_datetime: 'datetime',
    str: 'string'
}


def main():

    # Initialize time counter.
    global_start = time()

    # Initialize RDD container that will union with all upcoming RDDs.
    # TODO switch to column-by-column output?
    master_rdd = sc.emptyRDD()

    # Read input data.
    data = read_data(spark=spark, file=argv[1])

    # Separate columns that are in :data from those that are not.
    valid_columns = validate_request(user_columns=argv[2:], data_columns=data.columns)

    # Analyze valid columns only.
    for index, column in enumerate(valid_columns):
        iter_start = time()

        # Generate RDD containing (row_index, column_name, value, base_type, semantic_type, invalid) tuples.
        column_rdd = analyze(column, data)
        # TODO rdd_to_csv(column_rdd, column)

        # Take the union between the previous (or empty) RDD and the new RDD.
        master_rdd = master_rdd.union(column_rdd)

        # Print per-column time diagnostics.
        print('Column {} `{}` took {:.1f} seconds.'.format(index, column, time() - iter_start))

    # Output our all-in-one RDD containing RDDs that represent
    # each column's values to a CSV.
    rdd_to_csv(master_rdd)

    # Print global runtime diagnostics.
    print('Total runtime: {:.1f} seconds.'.format(time() - global_start))


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

    # First things, first: index our column.
    indexed_rdd = index_rdd(column_data)

    # Check column against base types and report column base type evaluation.
    base_type_rdd = analyze_base_type(indexed_rdd)

    # Check column against semantic types.
    semantic_type_results = analyze_semantic_type(base_type_rdd, column_name)

    # Output RDD containing semantic type information.
    return semantic_type_results


def analyze_base_type(data):
    """Perform initial base-type check of each column's values.

    This function performs a column-wise evaluation of the base-type distribution.
    Specifically, for each :base_type in :potential_base_types (the global
    list of base types), we attempt to cast that column'e values to :base_type.
    Once we do so, the origin data splits: send the valid casts to :valid
    and the invalid casts (the ones that didn't pass) to :remaining.

    For example, on the first iteration :valid will store the valid values
    in :data that were cast to int. Then, we take the union between our
    empty RDD :result_rdd and :valid to incrementally build our returned
    RDD.

    We continue with :remaining as our new origin dataset. Once we've
    exhausted all :base_type in :potential_base_type, we attribute
    any remaining values as strings.

    What's nice about this implementation is that we reduce the work done
    with each potential base type -- by the time we get to str() we've
    gone through int(), float(), and datetime(), so integer-only columns
    take almost no time for the remaining type checks. A downside is that
    string-only columns will go through all castings.

    :param data: Spark DataFrame containing one column's values.
    :return result_rdd: RDD of 3-tuples (row_index, value, base_type).
    """

    # Initialize results container.
    result_rdd = sc.emptyRDD()

    # For each function in our list of potential base type check functions
    for base_type_function in potential_base_types:

        # Attempt to cast our column's values as one of base_type_function (int, float, datetime).
        # This returns an RDD :base_type_rdd of (row_index, (Boolean, original_value)) tuples that are True
        # if and only if that value was successfully cast to that type.
        base_type_rdd = data.map(lambda row: (row[0], base_type_function(row[1]))).cache()

        # Split our data into :valid and :remaining RDDs. As their name suggests,
        # :valid are rows that were correctly cast with :base_type_function, and
        # :remaining are rows that weren't.
        valid, remaining = (
            (
                # From rows of (row_index, (True, val)) tuples, extract the :val,
                # re-map to (row_index, val, base_type) tuples.
                base_type_rdd.filter(lambda row: row[1][0] is True)
                             .map(lambda row: (row[0], row[1][1], base_type__names[base_type_function]))
            ),
            (
                # From rows of (row_index, (False, val)) tuples, we need to
                # recycle them through the casting functions. We thus have to
                # make sure that when we access row[1] that we access the
                # correct value and not `False`. So, we map our
                # (row_index, (False, val)) pairs to (row_index, val) in order
                # to correctly maintain our loop invariant.
                base_type_rdd.filter(lambda row: row[1][0] is False)
                             .map(lambda row: (row[0], row[1][1]))
            )
        )

        # Append the valid-cast tuples to our container RDD :result_rdd.
        result_rdd = result_rdd.union(valid)

        # Assign our origin dataset to our :remaining RDD.
        data = remaining

    # When we're done, we're left with (row_index, val) tuples that weren't able to be cast
    # as any of our defined base type functions. Hence, we default their casting to
    # string types since all types can be represented as strings. We don't do anything
    # new -- it's the same algorithm as we use for :valid.
    result_rdd = result_rdd.union(
        data.map(lambda row: (row[0], row[1], base_type__names[str]))
    )

    return result_rdd


def analyze_semantic_type(base_type_rdd, column_name):
    """Perform intermediary analysis on column's semantic type.

    :param base_type_rdd: RDD containing (val, cast) tuples as a result
        of initial pass through :analyze_base_type().
    :param column_name: string representation of column name. Used to
        properly populate our RDD tuples.
    :return semantic_type_rdd: RDD containing additional field per tuple representing our
        value's inferred semantic type.
    """

    # Determine column's most prevalent base type.
    dominant_base_type_name = get_dominant_base_type(base_type_rdd)

    # Mark all non-:dominant_base_type values as semantic type: unknown.
    unknown, remaining = (

        # For values where the base type is not the column's dominant base type,
        # their semantic type is unknown and validity is n/a. Finally, flatten the tuple
        # from ([row_index, column_name], val, type, (sem_type, valid))
        # to   (row_index, column_name, val, type, sem_type, valid)
        base_type_rdd.filter(lambda row: row[-1] != dominant_base_type_name)
                     .map(lambda row: ([row[0], column_name], row[1:], ('unknown', 'n/a')))
                     .map(lambda row: tuple(chain(*row))),

        # For values where the base type is the column's dominant base type,
        # evaluate their semantic type and validity. Finally, flatten the tuple
        # from ([row_index, column_name], val, type, (sem_type, valid))
        # to   (row_index, column_name, val, type, sem_type, valid).
        base_type_rdd.filter(lambda row: row[-1] == dominant_base_type_name)
                     .map(lambda row: ([row[0], column_name], row[1:], semantic_type_pipeline(dominant_base_type_name, row[0])))
                     .map(lambda row: tuple(chain(*row)))
    )

    # Append valid semantic type RDD :remaining to :unknown and return complete RDD.
    semantic_type_rdd = unknown.union(remaining)
    return semantic_type_rdd


if __name__ == '__main__':

    # Provide information on what this script can do.
    #   $ spark-submit faq.py
    if len(argv) == 1:
        cli_help()

    # Provide information on what columns are available for analysis.
    #   $ spark-submit faq.py <input_file>.csv
    elif len(argv) == 2:
        header, filename = get_header(argv[1])
        print_prompt(header, filename)

    # Perform analysis on desired columns (or all columns, if :all passed)
    #   $ spark-submit faq.py <input_file>.csv :all
    #   $ spark-submit faq.py <input_file>.csv 'column 1' ... 'column n'
    else:
        main()
