from scripts.aggregate_funcs import aggregate_funcs_list
from scripts.utils import read_data

from sys import argv
import os

from pyspark.sql import SparkSession
from pyspark.context import SparkContext

# Establish Spark session and context.
spark = SparkSession.builder \
                    .master("local") \
                    .appName("311 Analysis") \
                    .getOrCreate()

sc = SparkContext.getOrCreate()


def main():

    # Read in our first argument, which is a CSV with headers.
    data = read_data(spark, argv[1])

    # Iterate over our defined aggregate functions.
    for function in aggregate_funcs_list:

        # Obtain the query and name belonging to each one.
        temp_df, name, header = function(spark, data)

        # Store our DF :temp_df as a CSV. Easy!
        results_to_csv(temp_df, name, header)


def results_to_csv(df, name, header):

    path = 'data/' + name

    df.write.csv(os.path.join(path))

    with open(path + '/header.txt', 'w') as f:
        f.write(header)


if __name__ == '__main__':
    main()
