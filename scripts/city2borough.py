from functools import partial
import os

NEIGHBORHOODS_CSV_FPATH = '../data/wiki_Neighborhoods_in_New_York_City.csv'

def import_neighborhoods(fname):
    '''
    import wikipedia neighborhoods file
    returns: dictionary where key is borough
        and value is list of neighborhoods
    '''
    with open(fname,'r') as f:
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
    boroughs_dict = {'QUEENS':boroughs['Queens']+['QUEENS'],
                     'BROOKLYN':boroughs['Brooklyn']+['BROOKLYN'],
                     'MANHATTAN':boroughs['Manhattan']+['MANHATTAN','NEW YORK'],
                     'STATEN ISLAND':boroughs['Staten Island']+['STATEN ISLAND'],
                     'BRONX':boroughs['Bronx']+['BRONX']}
    return boroughs_dict

def _city2borough(borough_city_tuple,neighborhoods):
    '''
    If borough is in the borough_list, return it
    Otherwise, apply the city-to-borough map
    '''
    orig_borough,city = borough_city_tuple
    borough_list = ['QUEENS','BROOKLYN','BRONX','STATEN ISLAND','MANHATTAN']
    try: #convert to upper if it's a string
        if str.upper(orig_borough) in borough_list:
            return str.upper(orig_borough)
    except: #if orig_borough not a string, ignore error
        pass
    else: #if 
        #Return first borough that comes up
        for borough,hood_list in neighborhoods.iteritems():
            try:
                if str.upper(city) in hood_list:
                    return borough
            except:
                return None
        return None

neighborhoods = import_neighborhoods(NEIGHBORHOODS_CSV_FPATH)
city2borough = partial(_city2borough,neighborhoods=neighborhoods)

if __name__ == '__main__':
    for test_city in [('Queens','staten island'),('staten','BROOKLYN'),('Unspecified',5),(None,None),('Unspecified','Boerum Hill')]:
        print test_city,city2borough(test_city)