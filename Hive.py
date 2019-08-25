"""
Practice using Hive
"""
# this code should be executed within beeline

"""
Creating airports table
"""
# Copying file to sandbox
scp -P 2222 D:\Prasanna\Nootus\DataScience\Practice\Python\airports.csv maria_dev@localhost:/home/maria_dev/data

# creating temp airports table
CREATE TABLE airports_csv (id INT, ident STRING, type STRING, name STRING, latitude_deg DECIMAL(12, 8), longitude_deg DECIMAL(12, 8), elevation_ft INT, continent STRING, iso_country STRING, iso_region STRING, municipality STRING, scheduled_service STRING, gps_code STRING, iata_code STRING, local_code STRING, home_link STRING, wikipedia_link STRING, keywords STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties 
(
    "separatorChar" = ','
   ,"quoteChar"     = '"'
)  
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1")

# for beeline, first create a HDFS file
hadoop fs -copyFromLocal '/home/maria_dev/data/airports.csv' '/tmp/data'

# inserting into the temp table
LOAD DATA INPATH '/tmp/data/airports.csv' INTO TABLE airports_csv

# checking temp table and data
SELECT * FROM airports_csv LIMIT 10

# creating ORC table
CREATE TABLE airports (id INT, ident STRING, type STRING, name STRING, latitude_deg DECIMAL(12, 8), longitude_deg DECIMAL(12, 8), elevation_ft INT, continent STRING, iso_country STRING, iso_region STRING, municipality STRING, scheduled_service STRING, gps_code STRING, iata_code STRING, local_code STRING, home_link STRING, wikipedia_link STRING, keywords STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC


# inserting from temp table
INSERT INTO airports SELECT * FROM airports_csv

# checking data
SELECT * FROM airports LIMIT 10

# dropping temp csv table
DROP TABLE airports_csv

"""
Creating airport_freq table
"""
scp -P 2222 D:\Prasanna\Nootus\DataScience\Practice\Python\airport-frequencies.csv maria_dev@localhost:/home/maria_dev/data

CREATE TABLE airport_freq_csv (id INT, airport_ref INT, airport_ident STRING, type STRING, description STRING, frequency_mhz DECIMAL(8, 4))
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties 
(
    "separatorChar" = ','
   ,"quoteChar"     = '"'
)  
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1")

hadoop fs -copyFromLocal '/home/maria_dev/data/airport-frequencies.csv' '/tmp/data'

LOAD DATA INPATH '/tmp/data/airport-frequencies.csv' INTO TABLE airport_freq_csv

SELECT * FROM airport_freq_csv LIMIT 10

CREATE TABLE airport_freq (id INT, airport_ref INT, airport_ident STRING, type STRING, description STRING, frequency_mhz DECIMAL(8, 4))
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC

INSERT INTO airport_freq SELECT * FROM airport_freq_csv

SELECT * FROM airport_freq LIMIT 10

# dropping temp csv table
DROP TABLE airport_freq_csv


"""
--- SELECT, WHERE, DISTINCT, LIMIT
"""
# select * from airports limit 3	
select * from airports limit 3

# select id from airports where ident = 'KLAX'	
select id from airports where ident = 'KLAX'

# select distinct type from airports	
select distinct type from airports

"""
--- SELECT with multiple conditions
"""
# select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'
select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'

# select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'	
select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'	

"""
--- ORDER BY
"""
# select * from airport_freq where airport_ident = 'KLAX' order by type	
select * from airport_freq where airport_ident = 'KLAX' order by type	

# select * from airport_freq where airport_ident = 'KLAX' order by type desc	
select * from airport_freq where airport_ident = 'KLAX' order by type desc	

"""
--- IN… NOT IN
"""
# select id, ident, type from airports where type in ('heliport', 'balloonport')	
select * from airports where type in ('heliport', 'balloonport')	

# select * from airports where type not in ('heliport', 'balloonport')	
select * from airports where type not in ('heliport', 'balloonport')	

"""
--- GROUP BY, COUNT, ORDER BY
"""
# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type	
select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type	

# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc	
select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc	

"""
-- HAVING
"""
# select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc	
select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc	

"""
-- Top N records
"""
# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10	
select iso_country, count(*) size from airports group by iso_country order by count(*) desc limit 10

# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10
select iso_country, count(*) size from airports group by iso_country order by count(*) desc limit 10 offset 11

"""
-- Aggregate functions (MIN, MAX, MEAN)
"""
# select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports	
select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports

"""
-- JOIN
"""
# select airport_ident, type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'	
select airport_ident, af.type, description, frequency_mhz from airport_freq af 
    join airports a on af.airport_ref = a.id where a.ident = 'KLAX'	
