from functools import partial
import os
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

NEIGHBORHOODS_CSV_FPATH = '../data/wiki_Neighborhoods_in_New_York_City.csv'

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

def clean_borough(df):
    '''
    Clean up the borough field of complaints dataframe
    '''
    # UDF of our :city2borough function.
    udf_city = udf(city2borough, StringType())
    # Replace original 'Borough' column with our inferred borough.
    new_df = df.withColumn("Borough", udf_city(df['City']))
    
    return new_df


if __name__ == '__main__':
    for test_city in ['Queens','staten island','staten','BROOKLYN','Unspecified',5,None,'Boerum Hill']:
        print(test_city,city2borough(test_city))