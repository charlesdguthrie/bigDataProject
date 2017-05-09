import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import pandas as pd
from city2borough import clean_borough

FNAME_311 = '../data/311-all.csv'
#FNAME_311 = '../data/311_sample_head.csv'
BUILDING_DATADIR = '../data/BORO_zip_files_csv/'

spark = SparkSession.builder \
                .master("local") \
                .appName("311 Analysis") \
                .getOrCreate()

#Strip whitespace and convert string to upper
clean_address = F.udf(lambda address: address.upper().strip() if address else None, StringType())

def clean_complaints_df(cdf):
    #clean Borough field to replace unspecified with borough
    cdf = clean_borough(cdf)
    # 1. Filter for only rows where we have a HEATING or HEAT/HOT WATER complaint
    hdf = cdf.filter(cdf["Complaint Type"].like('%HEAT%'))
    #clean "Incident Address" and create new column called "Address"
    hdf = hdf.withColumn('Address', clean_address('Incident Address'))
    return hdf


#Import building age datasets
#DEPRECATED: bdf = spark.read.csv('Buildings_Clean.csv', header=True)
#TODO if time: wget, import and unzip file
# zip_ref = zipfile.ZipFile('../data/BORO_zip_files_csv.zip', 'r')
# zip_ref.extractall('../data/')
# zip_ref.close()
def load_building_files(datadir):
    mn = spark.read.csv(datadir+'MN.csv',header=True)
    qn = spark.read.csv(datadir+'QN.csv',header=True)
    bk = spark.read.csv(datadir+'BK.csv',header=True)
    si = spark.read.csv(datadir+'SI.csv',header=True)
    bx = spark.read.csv(datadir+'BX.csv',header=True)
    bdf = mn.unionAll(qn)\
            .unionAll(bk)\
            .unionAll(si)\
            .unionAll(bx)
    return bdf


def clean_building_df(bdf):
    '''
    select only Address, YearBuilt, and Borough,
    eliminate YearBuilt<=0,
    and change abbreviated boroughs to full borough names
    args:
        bdf: dataframe of PLUTO buildings data
    returns:
        bdf: cleaned dataframe
    '''
    bdf = bdf.select(['Address','YearBuilt','ZipCode','Borough']) \
             .filter(bdf['YearBuilt']>0)
    bdf = bdf.withColumn('Address', clean_address('Address'))
    borough_dict = {
        'MN':'MANHATTAN',
        'BX':'BRONX',
        'BK':'BROOKLYN',
        'SI':'STATEN ISLAND',
        'QN':'QUEENS'
    }
    unabbreviate = F.udf(lambda x: borough_dict[x], StringType())
    bdf = bdf.withColumn('Borough',unabbreviate('Borough'))
    return bdf

cdf = spark.read.csv(FNAME_311, header=True)
hdf = clean_complaints_df(cdf)
bdf = load_building_files(BUILDING_DATADIR)
bdf = clean_building_df(bdf)

#create merged mdf from joining hdf to bdf
mdf = hdf.join(bdf, on=['Address','Borough'], how='inner')

#Get building count by yearBuilt
totals = bdf.groupby(['YearBuilt']).agg({'*':'count'})
totals = totals.withColumnRenamed('count(1)', 'total')

#Get complaint count by YearBuilt
complaints = mdf.groupby(['YearBuilt']).agg({'*':'count'})
complaints = complaints.withColumnRenamed('count(1)','complaints')

result = totals.join(complaints, on='YearBuilt', how='inner')
# Write to CSV
result.toPandas().to_csv('../data/complaints_v_age.csv')