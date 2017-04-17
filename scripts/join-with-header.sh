cp header.txt master.csv
cat part-*.csv >> master.csv
rm part-*.csv .*.crc _SUCCESS
