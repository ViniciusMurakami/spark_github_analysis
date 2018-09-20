#!/bin/bash -x 

###################################################
# Downloading Github Repo Data
# https://www.gharchive.org/
# 
# TO-DO add the functionality to this script 
# to receive parameters for year/month and
# add the if function to validate the parameters
###################################################
#years=(2010 2011 2012 2013 2014 2015 2016 2017 2018)
years=(2017 2018)
months=(01 02 03 04 05 06 07 08 09 10 11 12)

for y in ${years[*]}
do
	for m in ${months[*]}
	do
		wget http://data.gharchive.org/$y-$m-{01..31}-{0..23}.json.gz
		hdfs dfs -mkdir /user/spark/spark_ds_github/$y/$m
		hdfs dfs -copyFromlocal ~/$y-$m-{01..31}-{0..23}.json.gz /user/spark/spark_ds_github/$y/$m/
	done
	sleep 60
done