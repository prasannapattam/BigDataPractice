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
    .map(lambda row: Row(id=row.id, ident=row.ident, type= row.type)).take(10)

# select * from airports where type not in ('heliport', 'balloonport')	
airports.filter(~airports.type.isin(["heliport", "balloonport"])) \
    .select(airports.id, airports.ident, airports.type).show(10)
