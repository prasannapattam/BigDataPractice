"""
Practicing using Spark RDD

@author: ppattam
"""
# this code should be executed within pyspark

# creating RDD files
airports = spark.read.csv("/tmp/data/airports.csv", header=True, inferSchema=True).rdd
airport_freq  = spark.read.csv("/tmp/data/airport-frequencies.csv", header=True, inferSchema=True).rdd

from pyspark.sql.types import Row

"""
--- SELECT, WHERE, DISTINCT, LIMIT
"""
# select * from airports limit 3	
airports.take(3)

# select ident, name, municipality from airports limit 3
airports.map(lambda row: Row(ident=row.ident, name=row.name, municipality=row.municipality)).take(3)

# select id from airports where ident = 'KLAX'
airports.filter(lambda row: row.ident == "KLAX").map(lambda row: Row(id=row.id)).collect()

# select distinct type from airports	
#airports.select(airports.type).distinct().show()
airports.map(lambda row: row.type).distinct().collect()


"""
--- SELECT with multiple conditions
"""
# select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'
airports.filter(lambda row: (row.iso_region == "US-CA") & (row.type == "seaplane_base")).collect()

# select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'	
airports.filter(lambda row: (row.iso_region == "US-CA") & (row.type == "large_airport")) \
    .map(lambda row: Row(ident=row.ident, name=row.name, municipality=row.municipality)) \
    .collect()

"""
--- ORDER BY
"""
# select * from airport_freq where airport_ident = 'KLAX' order by type	
airport_freq.filter(lambda row: row.airport_ident == "KLAX") \
    .sortBy(lambda row: row.type).collect()

# select * from airport_freq where airport_ident = 'KLAX' order by type desc	
airport_freq.filter(lambda row: row.airport_ident == "KLAX") \
    .sortBy(lambda row: row.type, ascending=False).collect()

"""
--- IN… NOT IN
"""

# select * from airports where type in ('heliport', 'balloonport')	
airports.filter(lambda row: row.type in (["heliport", "balloonport"])) \
    .map(lambda row: Row(id=row.id, ident=row.ident, type= row.type)) \
    .take(10)


# select * from airports where type not in ('heliport', 'balloonport')	
airports.filter(lambda row: row.type not in (["heliport", "balloonport"])) \
    .map(lambda row: Row(id=row.id, ident=row.ident, type=row.type)) \
    .take(10)

"""
--- GROUP BY, COUNT, ORDER BY
"""
# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type	
airports.map(lambda row: ((row.iso_country, row.type), 1)) \
    .reduceByKey(lambda accum, curr: accum + curr) \
    .take(10)

# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc	
airports.map(lambda row: ((row.iso_country, row.type), 1)) \
    .reduceByKey(lambda accum, curr: accum + curr) \
    .sortBy(lambda row: row[1], ascending=False) \
    .take(10)

"""
-- HAVING and WHERE
"""
# select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc	
airports.filter(lambda row: row.iso_country == "US") \
    .map(lambda row: (row.type, 1)) \
    .reduceByKey(lambda accum, curr: accum + curr) \
    .filter(lambda row: row[1] > 1000) \
    .sortBy(lambda row: row[1], ascending=False) \
    .take(10)

"""
-- Top N records
"""
# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10	
airports.map(lambda row: (row.iso_country, 1)) \
    .reduceByKey(lambda accum, curr: accum + curr) \
    .sortBy(lambda row: row[1], ascending=False) \
    .take(10)


# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10
airports.map(lambda row: (row.iso_country, 1)) \
    .reduceByKey(lambda accum, curr: accum + curr) \
    .sortBy(lambda row: row[1], ascending=False) \
    .rangeBetween(10, 15) \
    .take(20)


"""
# -- Aggregate functions (MIN, MAX, AVG)
"""
# select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports	
# airports.agg({"elevation_ft": { "Max": np.max, "Min": np.min, "Avg": np.mean }}).T

airports.map(lambda row: row.elevation_ft if row.elevation_ft else 0) \
    .map(lambda key: (key, key, key, 1, 1)) \
    .reduce(lambda accu, curr: (max(accu[0], curr[0]), min(accu[1], \
        curr[1]), accu[2] + curr[2], accu[3] + curr[3], (accu[2] + curr[2])/(accu[3] + curr[3])))

"""
-- JOIN
"""
# select airport_ident, airport_freq.type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'	
airport_freq.map(lambda row: (row.airport_ref, (row.airport_ident, row.type, row.description, row.frequency_mhz))) \
    .join(airports.map(lambda row: (row.id, row.ident))) \
    .filter(lambda x: x[1][1] == "KLAX") \
    .collect()

