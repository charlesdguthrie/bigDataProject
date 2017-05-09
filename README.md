# bigDataProject
Repository for our DS1004 Big Data Project

## Data Provenance

Our data was sourced through the NYC OpenData portal. The 311 data is segmented into two datasets with identical fields covering the years of (1) 2009 and (2) 2010 to the present

We took the following steps to generate `311-all.csv`, which is the aggregate dataset:

1. Validate that there is no difference in features between the dataset.

2. Copy the 2009 dataset in its entirety to a new file `311-all.csv`.

	```bash
	$ cp 311-2009.csv 311-all.csv
	```
3. Append the 2010 and onwards data (excluding the header) to our new `311-all.csv`

	```bash
	# Start tail 2 lines after 0, so exclude line 1 (header)
	$ tail +2 311-2010_now.csv >> 311-all.csv
	```

Afterwards, we have a new, aggregated `311-all.csv`. Checking its size and line count:

```bash
$ ls -lh 311-all.csv
-rw-r--r--  1 danny  staff   7.3G Apr  3 22:02 311-all.csv
$ wc -l 311-all.csv
15358922 311-all.csv
```

Hence, our dataset is around 7.3 GB and contains 15,358,921 observations.

A few other steps we took:

1. After downloading a TSV of New York State information from https://www.unitedstateszipcodes.org/ny/, we isolated the ZIP code field in order to perform a semantic type evaluation on the validity of any particular ZIP code. We did this with the following:

	```bash
	$ tail -n +2 nys_zips_clean.tsv | cut -f 1 > nys_zips.txt 
	```

2. Latitude-Longitude bounds for New York State came from Wikipedia: https://en.wikipedia.org/wiki/List\_of\_extreme\_points\_of\_U.S.\_states



## Part 1: Data Exploration
`faq` assumes you don't know anything about using it, so we baked in some hand-holding to get you started. Start by running the following from the terminal:

```bash
$ spark-submit faq.py
```

Assuming you have all required dependencies (Spark), this should output a helpful prompt. This prompt will tell you how to proceed, but basically reduces to the following:

1. With only an input CSV, `faq` will give you a pretty-print of the column names:

  ```bash
  $ spark-submit faq.py <input-file>.csv
  ```

2. With both an input CSV *and* space-separated strings, `faq` will perform the actual analysis on the valid columns:

  ```bash
  $ spark-submit faq.py <input-file>.csv 'column 1' 'column 2'
  ```

3. With an input CSV and the special keyword `:all`, `faq` will perform the analysis on all columns:

  ```bash
  $ spark-submit faq.py <input-file>.csv :all
  ```

## Part 2: Hypotheses and Analysis
```building_age.py``` joins the 311-Complaints the NYC PLUTO dataset, which includes age of buildings, to produce average number of heating complaints per building vs. the year it was built.  This is part of the hypothesis that older buildings would be more likely to have heating complaints.  

```city2borough.py``` cleans the ```Borough``` field to fill in missing boroughs using other indicators of location from the dataset.