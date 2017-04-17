# COLUMNS INCLUDE:
#   id: integer column that can be used to identify a row from the original dataset.
#       If you isolate one id (say, 1000), that id will return 52 rows -- each of which
#       corresponding to one value from the original dataset.
#   column_name: string column holding name of the column derived from original dataset.
#   value: (int/float/datetime/string/None) value from original dataset.
#   base_type: string column holding name of :value's base type.
#   semantic_type: string column holding name of :value's semantic type.
#   is_valid: string column representing whether :value is a valid value for our dataset.


def aggregate_base_type(spark, data):
    """Descriptive statistics on base type composition for each column.

    :param spark: SparkSession object used for querying :data DataFrame.
    :param data: Spark DataFrame containing our DF output from `faq.py`.
    :return temp_df: new DataFrame after applying Spark SQL query onto :data.
    :return name: name used to identify our dataset.
    """

    column_count = 52

    # This query gives us a break-down of what base types were reported
    # within each column, and for each column what percent of that column
    # was cast as each base type.
    query, name, header = (
        """
        SELECT
            column_name,
            base_type,
            (COUNT(*)/{}) * 100 AS frequency_percentile
        FROM
            data
        GROUP BY
            column_name,
            base_type
        ORDER BY
            column_name
        """.format(data.count() / column_count),
        'base-type-aggregate',
        'column_name,base_type,frequency_percentile\n'
    )

    # Create a view of our data under alias `data` and query that table.
    data.createOrReplaceTempView("data")
    temp_df = spark.sql(query)

    # Return the DataFrame and the query name to our iterator sequence.
    return temp_df, name, header


def aggregate_base_type_2(spark, data):

    row_count = 15358921

    query, name, header = (
        """
        SELECT
            column_name,
            SUM(
                CASE
                    WHEN 'base_type' = 'int' THEN 1 ELSE 0
                END
            )/{0} AS percent_int,
            SUM(
                CASE
                    WHEN 'base_type' = 'float' THEN 1 ELSE 0
                END
            )/{0} AS percent_float,
            SUM(
                CASE
                    WHEN 'base_type' = 'datetime' THEN 1 ELSE 0
                END
            )/{0} AS percent_datetime,
            SUM(
                CASE
                    WHEN 'base_type' = 'string' THEN 1 ELSE 0
                END
            )/{0} AS percent_string
        FROM
            data
        GROUP BY
            column_name
        """.format(row_count),
        'column_base_type_breakdown',
        'column_name,percent_int,percent_float,percent_datetime,percent_string'
    )

    # Create a view of our data under alias `data` and query that table.
    data.createOrReplaceTempView("data")
    temp_df = spark.sql(query)

    # Return the DataFrame and the query name to our iterator sequence.
    return temp_df, name, header



def column_statistics(spark, data):

    query, name, header = (
        """
        SELECT
            column_name,
            semantic_type,
            SUM(
                CASE
                    WHEN is_valid = 'valid' THEN 1
                    ELSE 0
                END
            ) AS count_valid_semantic,
            SUM(
                CASE
                    WHEN is_valid = 'invalid' THEN 1
                    ELSE 0
                END
            ) AS count_invalid_semantic,
            SUM(
                CASE
                    WHEN is_valid = 'n/a' THEN 1
                    ELSE 0
                END
            ) AS count_NA_semantic,
        FROM
            data
        GROUP BY
            column_name,
            semantic_type
        ORDER BY
            column_name
        """,
        'column-statistics',
        'column_name,semantic_type,count_valid_semantic,count_invalid_semantic,count_NA_semantic'
    )

    # Create a view of our data under alias `data` and query that table.
    data.createOrReplaceTempView("data")
    temp_df = spark.sql(query)

    # TODO
    # most_common = data.rdd.map(lambda row: ((row[1], row[2]), 1)) \
    #                       .reduceByKey(lambda a, b: a + b)

    # Return the DataFrame and the query name to our iterator sequence.
    return temp_df, name, header


def most_common_value(spark, data):
    pass



# Store each of our aggregate functions for simplified calling.
aggregate_funcs_list = [aggregate_base_type]
