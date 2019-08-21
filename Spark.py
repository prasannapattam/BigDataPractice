"""
Practicing using Spark

@author: ppattam
"""
# this code should be executed within pyspark

"""
Creating airports & airport_freq dataframe
"""

# first create a HDFS file
hadoop fs -copyFromLocal "/home/maria_dev/data/airports.csv" "/tmp/data"
hadoop fs -copyFromLocal "/home/maria_dev/data/airport-frequencies.csv" "/tmp/data"

# creating dataframes
airports = spark.read.csv("/tmp/data/airports.csv", header=True, inferSchema=True)
airport_freq  = spark.read.csv("/tmp/data/airport-frequencies.csv", header=True, inferSchema=True)


"""
--- SELECT, WHERE, DISTINCT, LIMIT
"""
# select * from airports limit 3	
airports.limit(3).show()

# select id from airports where ident = 'KLAX'
airports.filter(airports.ident == "KLAX").select(airports.id).show()

# select distinct type from airports	
airports.select(airports.type).distinct().show()

"""
--- SELECT with multiple conditions
"""
# select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'
airports.filter((airports.iso_region == "US-CA") & (airports.type == "seaplane_base")) \
    .show()

# select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'	
airports.filter((airports.iso_region == "US-CA") & (airports.type == "large_airport")) \
    .select(airports.ident, airports.name, airports.municipality) \
    .show()

"""
--- ORDER BY
"""
# select * from airport_freq where airport_ident = 'KLAX' order by type	
airport_freq.filter(airport_freq.airport_ident == "KLAX") \
    .orderBy(airport_freq.type).show()

# select * from airport_freq where airport_ident = 'KLAX' order by type desc	
airport_freq.filter(airport_freq.airport_ident == "KLAX") \
    .orderBy(airport_freq.type.desc()).show()


"""
--- IN… NOT IN
"""

# select * from airports where type in ('heliport', 'balloonport')	
airports.filter(airports.type.isin(["heliport", "balloonport"])) \
    .select(airports.id, airports.ident, airports.type).show(10)

# select * from airports where type not in ('heliport', 'balloonport')	
airports.filter(~airports.type.isin(["heliport", "balloonport"])) \
    .select(airports.id, airports.ident, airports.type).show(10)

"""
--- GROUP BY, COUNT, ORDER BY
"""
# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type	
airports.groupBy([airports.iso_country, airports.type]).count().show()

# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc	
airports.groupBy([airports.iso_country, airports.type]).count() \
    .orderBy([airports.iso_country, "count"], ascending=[True, False]).show()

"""
-- HAVING and WHERE
"""
# select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc	
airports.filter(airports.iso_country == "US").groupby(airports.type).count() \
    .filter( "count > 1000").orderBy("count", ascending=False).show()

"""
-- Top N records
"""
# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10	
airports.groupBy(airports.iso_country).count() \
    .orderBy("count", ascending=False).limit(10).show()

# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10
airports.groupBy(airports.iso_country).count() \
    .orderBy("count", ascending=False).limit(20).orderBy("count") \
    .limit(10).orderBy("count", ascending=False).show()

"""
# -- Aggregate functions (MIN, MAX, AVG)
"""
# select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports	
from pyspark.sql import functions as F
airports.agg(F.max(airports.elevation_ft), F.min(airports.elevation_ft), F.avg(airports.elevation_ft)).show()

"""
-- JOIN
"""
# select airport_ident, type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'	
airport_freq.join(airports, airport_freq.airport_ref == airports.id) \
            .filter(airports.ident == "KLAX") \
            .select(airports.ident, airport_freq.type, airport_freq.description, \
                    airport_freq.frequency_mhz).show()
                  
