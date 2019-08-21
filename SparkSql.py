"""
Practice using Spark SQL
"""

# first create a HDFS file
hadoop fs -copyFromLocal "/home/maria_dev/data/airports.csv" "/tmp/data"
hadoop fs -copyFromLocal "/home/maria_dev/data/airport-frequencies.csv" "/tmp/data"

# creating airports file
airports = spark.read.csv("/tmp/data/airports.csv", header=True, inferSchema=True)
airport_freq  = spark.read.csv("/tmp/data/airport-frequencies.csv", header=True, inferSchema=True)

# creating TempView
airports.createOrReplaceTempView("airports")
airport_freq.createOrReplaceTempView("airport_freq")

"""
--- SELECT, WHERE, DISTINCT, LIMIT
"""
# select * from airports limit 3	
spark.sql("select * from airports limit 3").show()

# select id from airports where ident = 'KLAX'
spark.sql("select id from airports where ident = 'KLAX'").show()


# select distinct type from airports	
spark.sql("select distinct type from airports").show()

"""
--- SELECT with multiple conditions
"""
# select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'
spark.sql("select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'").show()

# select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'	
spark.sql("select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'").show()

"""
--- ORDER BY
"""
# select * from airport_freq where airport_ident = 'KLAX' order by type	
spark.sql("select * from airport_freq where airport_ident = 'KLAX' order by type").show()

# select * from airport_freq where airport_ident = 'KLAX' order by type desc	
spark.sql("select * from airport_freq where airport_ident = 'KLAX' order by type desc").show()

"""
--- IN… NOT IN
"""

# select * from airports where type in ('heliport', 'balloonport')	
spark.sql("select * from airports where type in ('heliport', 'balloonport')").show(10)

# select * from airports where type not in ('heliport', 'balloonport')	
spark.sql("select * from airports where type not in ('heliport', 'balloonport')").show(10)

"""
--- GROUP BY, COUNT, ORDER BY
"""
# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type	
spark.sql("select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type").show()

# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc	
spark.sql("select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc").show()

"""
-- HAVING and WHERE
"""
# select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc	
------------ Issue: Order by not supported
spark.sql("select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000").show()


"""
-- Top N records
"""
# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10	
spark.sql("select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10").show()

# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10
----------- Offset not supported
spark.sql("select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10").show()

"""
# -- Aggregate functions (MIN, MAX, AVG)
"""
# select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports	
spark.sql("select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports").show()

"""
-- JOIN
"""
# select airport_ident, airport_freq.type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'	
spark.sql("select airport_ident, airport_freq.type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'").show()

