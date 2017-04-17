# Initial spark-submit job on faq.py. Takes path to data CSV file.
echo '---------------------------'
echo 'Running spark-submit faq.py'
echo '---------------------------'
/Users/danny/spark-2.0.1-bin-hadoop2.7/bin/spark-submit faq.py $1 :all

# Upon completion, move join-with-header.sh to the data directory
# and run it.
echo '---------------------------'
echo 'Moving files to join master'
echo '---------------------------'
cp scripts/join-with-header.sh data
cd data
./join-with-header.sh
cd ..

# Now that master.csv is a CSV with a header, pipe that into aggregate.py.
echo '---------------------------------'
echo 'Running spark-submit aggregate.py'
echo '---------------------------------'
/Users/danny/spark-2.0.1-bin-hadoop2.7/bin/spark-submit aggregate.py data/master.csv

cd data

# aggregate.py will output multiple directories (one per aggregate function),
# so for each one copy join-with-header.sh into their directory and run it.
cp join-with-header.sh */

echo '----------------------------------------'
echo 'Joining all nested aggregate directories'
echo '----------------------------------------'

for dir in */;
do
  cd "$dir"
  ./join-with-header.sh
done
