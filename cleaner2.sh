#/bin/sh

for f in `ls 0*HourlyCOG.csv`
do
echo 'Processing  '$f
	python cleaner.py $f
done
