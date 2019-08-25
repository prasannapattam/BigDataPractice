"""
Practice using HBase and Phoenix
"""

CREATE TABLE AIRPORTS(id INTEGER PRIMARY KEY, ident VARCHAR, 
                           type VARCHAR, name VARCHAR, latitude_deg DECIMAL(12, 8), 
                           longitude_deg DECIMAL(12, 8), elevation_ft INTEGER, 
                           continent VARCHAR, iso_country VARCHAR, iso_region VARCHAR, 
                           municipality VARCHAR, scheduled_service VARCHAR, 
                           gps_code VARCHAR, iata_code VARCHAR, local_code VARCHAR, 
                           home_link VARCHAR, wikipedia_link VARCHAR, keywords VARCHAR)


psql.py '/home/maria_dev/data/airports.csv'

CREATE TABLE AIRPORT_FREQ (id INTEGER PRIMARY KEY, airport_ref INTEGER, 
                           airport_ident VARCHAR, type VARCHAR, description VARCHAR, 
                           frequency_mhz DECIMAL(8, 4))

psql.py -t AIRPORT_FREQ '/home/maria_dev/data/airport-frequencies.csv'


"""
--- SELECT, WHERE, DISTINCT, LIMIT
"""
# select * from airports limit 3	
select * from airports limit 3	 # returning based on the stored order

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
select id, ident, type from airports where type in ('heliport', 'balloonport')

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
select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10

# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10
select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10

"""
-- Aggregate functions (MIN, MAX, MEAN)
"""
# select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports	
select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports

"""
-- JOIN
"""
# select airport_ident, type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'	
select airport_ident, airport_freq.type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'

