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
        """.format(data.count() / 52),
        'base-type-aggregate',
        'column_name,base_type,frequency_percentile\n'
    )

    # Create a view of our data under alias `data` and query that table.
    data.createOrReplaceTempView("data")
    temp_df = spark.sql(query)

    # Return the DataFrame and the query name to our iterator sequence.
    return temp_df, name, header


# Store each of our aggregate functions for simplified calling.
aggregate_funcs_list = [aggregate_base_type]
