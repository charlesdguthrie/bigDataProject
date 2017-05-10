
# coding: utf-8

# In[1]:

from functools import partial
import os

NEIGHBORHOODS_CSV_FPATH = 'data/wiki_Neighborhoods_in_New_York_City.csv'

def import_neighborhoods(fname):
    '''
    import wikipedia neighborhoods file
    returns: dictionary where key is borough
        and value is list of neighborhoods
    '''
    with open(fname, 'r') as f:
        raw = f.read()
    
    lines = raw.split(os.linesep)
    
    assert len(lines)==59, "Neighborhoods file not proper length"
    
    boroughs = {}
    
    for line in lines:
        fields = line.split(',')
        borough = fields.pop(0)
        if borough not in boroughs:
            boroughs[borough]=[]
        for f in fields:
            if f:
                neighborhood = str.upper(f.strip())
                boroughs[borough].append(neighborhood)
    boroughs_dict = {'QUEENS': set(boroughs['Queens']+['QUEENS']),
                     'BROOKLYN': set(boroughs['Brooklyn']+['BROOKLYN']),
                     'MANHATTAN': set(boroughs['Manhattan']+['MANHATTAN','NEW YORK']),
                     'STATEN ISLAND': set(boroughs['Staten Island']+['STATEN ISLAND']),
                     'BRONX':set(boroughs['Bronx']+['BRONX'])}
    return boroughs_dict

def _city2borough(city, neighborhoods):
    '''
    If borough is in the borough_list, return it
    Otherwise, apply the city-to-borough map
    '''

    for borough, hood_list in neighborhoods.items():
        try:
            if str.upper(city) in hood_list:
                return borough
        except:
            return 'None'
    return 'None'

neighborhoods = import_neighborhoods(NEIGHBORHOODS_CSV_FPATH)
city2borough = partial(_city2borough, neighborhoods=neighborhoods)


# In[2]:

def clean_borough(df):
    
    # NOTE: DEFINE THESE OUTSIDE OF FUNCTION
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    # UDF of our :city2borough function.
    udf_city = udf(city2borough, StringType())
    
    # Replace original 'Borough' column with our inferred borough.
    new_df = df.withColumn("Borough", udf_city(df['City']))
    
    return new_df


# In[3]:

df = spark.read.csv("/Users/danny/Downloads/311-all.csv", header=True)
new_df = clean_borough(df)


# In[4]:

def reduce_dataset(df):
    
    cols = [
        'Unique Key',
        'Created Date',
        'Complaint Type',
        'Incident Zip',
        'Borough',
        'Latitude',
        'Longitude'
    ]
    
    reduced = df.select(cols)
    return reduced, cols


# In[5]:

reduced_df, header = reduce_dataset(new_df)


# In[7]:

reduced_df.select('Borough').groupBy('Borough').count().show()


# In[ ]:

reduced_df.write.csv("/Users/danny/Downloads/311-reduced")


# In[ ]:

with open('/Users/danny/Downloads/311-reduced/header.txt', 'w') as f:
    f.write(",".join(header) + "\n")


# In[ ]:

# cd into proper data directory
# $ cat header.txt > 311-reduced.csv
# $ cat part-*.csv >> 311-reduced.csv

