"""
Practice using Pig
"""
# this code should be executed within Pig command (grunt)

airports = LOAD '/tmp/data/airports.csv' USING PigStorage(',') AS (
            id:int, ident:chararray, type:chararray, name:chararray,
            latitude_deg:float, longitude_deg:float, 
            elevation_ft:int, continent:chararray, iso_country:chararray, 
            iso_region:chararray, municipality:chararray, scheduled_service:chararray, 
            gps_code:chararray, iata_code:chararray, local_code:chararray, 
            home_link:chararray, wikipedia_link:chararray, keywords:chararray);

airport_freq = LOAD '/tmp/data/airport-frequencies.csv' USING PigStorage(',') AS (
                    id:int, airport_ref:int, airport_ident:chararray, type:chararray, 
                    description:chararray, frequency_mhz:float);

"""
--- SELECT, WHERE, DISTINCT, LIMIT
"""
# select * from airports limit 3	
A = LIMIT airports 3;
DUMP A;

# select id from airports where ident = 'KLAX'	
A = FILTER airports BY ident == 'KLAX';
DUMP A;

# select distinct type from airports	
A = FOREACH airports GENERATE type;
B = DISTINCT A;
DUMP B; 

"""
--- SELECT with multiple conditions
"""
# select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'
A = FILTER airports BY iso_region == 'US-CA' and type == 'seaplane_base'
DUMP A;

# select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'	
A = FILTER airports BY iso_region == 'US-CA' and type == 'seaplane_base';
B = FOREACH A GENERATE ident, name, municipality;
DUMP B;

"""
--- ORDER BY
"""
# select * from airport_freq where airport_ident = 'KLAX' order by type	
A = FILTER airport_freq BY airport_ident == 'KLAX';
B = ORDER A BY type;
DUMP B;

# select * from airport_freq where airport_ident = 'KLAX' order by type desc	
A = FILTER airport_freq BY airport_ident == 'KLAX';
B = ORDER A BY type DESC;
DUMP B;


"""
--- IN… NOT IN
"""
# select * from airports where type in ('heliport', 'balloonport')	
A = FILTER airports BY type IN ('heliport', 'balloonport');
B = FOREACH A GENERATE id, ident, type;
C = LIMIT B 10;
DUMP C;

# select * from airports where type not in ('heliport', 'balloonport')	
A = FILTER airports BY NOT type IN ('heliport', 'balloonport');
B = FOREACH A GENERATE id, ident, type;
C = LIMIT B 10;
DUMP C;


"""
--- GROUP BY, COUNT, ORDER BY
"""
# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type	
A = GROUP airports BY (iso_country, type);
B = FOREACH A GENERATE group.iso_country, group.type, COUNT(airports);
C = LIMIT B 10;
DUMP C;


# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc	
A = GROUP airports BY (iso_country, type);
B = FOREACH A GENERATE group.iso_country, group.type, COUNT(airports) as cnt;
C = ORDER B BY iso_country, cnt DESC;
D = LIMIT C 10;
DUMP D;

"""
-- HAVING and WHERE
"""
# select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc	
A = FILTER airports BY iso_country == 'US';
B = GROUP A BY type;
C = FOREACH B GENERATE group as type, COUNT(A) as cnt;
D = FILTER C BY cnt > 1000;
E = ORDER D BY cnt DESC;
DUMP E;


"""
-- Top N records
"""
# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10	
A = GROUP airports BY iso_country;
B = FOREACH A GENERATE group as iso_country, COUNT(airports) as cnt;
C = ORDER B BY cnt DESC;
D = LIMIT C 10;
DUMP D;

# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10
A = GROUP airports BY iso_country;
B = FOREACH A GENERATE group as iso_country, COUNT(airports) as cnt;
C = ORDER B BY cnt DESC;
D = LIMIT C 20;
DUMP D;

"""
# -- Aggregate functions (MIN, MAX, AVG)
"""
# select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports	
A = GROUP airports ALL;
B = FOREACH A GENERATE MAX(airports.elevation_ft), MIN(airports.elevation_ft), AVG(airports.elevation_ft);
DUMP B;

"""
-- JOIN
"""
# select airport_ident, type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'	

A = JOIN airports BY id, airport_freq BY airport_ref;
B = FILTER A BY airports::ident == 'KLAX';
C = FOREACH B GENERATE airports::ident, airport_freq::type, airport_freq::description, airport_freq::frequency_mhz;
DUMP C
