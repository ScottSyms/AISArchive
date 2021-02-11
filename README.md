# AISArchive
Strategy for storing Automated Identity System data in Parquet files for retrieval

Software requires two inputs- a file with RAW NMEA AIS sentences and an archive directory to place the Parquet files.

Several AIS files can be aggregated into the same Parquet archive, or grouped into several files.  Query tools like Apache Drill can use either format.

I run the script with GNU parallel.

<code>time ls 2019-03-01*gz  | parallel --bar --joblog parallel.log python ~/code/archivetoparquet.py ~/data/archive/parquet/ {} </code>

which translates into- 
**time** time the overall execution
**ls 2019-03-01*gz** create a list of compressed files to process (in this instance, all files for March 1st, 2019). 

**parallel**  pass this list to GNU parallel which will run one instance of the script per core concurrently. 

**python ~/code/archivetoparquet.py** run the script

**~/data/archive/parquet/** directory to collect the parquet archive

**{}** substitute with the filename
