# -*- coding: utf-8 -*-
"""
Practise using Pandas

@author: ppattam
"""

import pandas as pd
import numpy as np

airports = pd.read_csv("airports.csv")
airport_freq = pd.read_csv("airport-frequencies.csv")

"""
--- SELECT, WHERE, DISTINCT, LIMIT
"""
# select * from airports	
airports

# select * from airports limit 3	
airports.head(3)

# select id from airports where ident = 'KLAX'	
airports[airports.ident == "KLAX"].id

# select distinct type from airports	
airports.type.unique()

"""
--- SELECT with multiple conditions
"""
# select * from airports where iso_region = 'US-CA' and type = 'seaplane_base'
airports[(airports.iso_region == "US-CA") & (airports.type == "seaplane_base")]

# select ident, name, municipality from airports where iso_region = 'US-CA' and type = 'large_airport'	
airports[(airports.iso_region == "US-CA") & (airports.type == "large_airport")][["ident", "name", "municipality"]]

"""
--- ORDER BY
"""
# select * from airport_freq where airport_ident = 'KLAX' order by type	
airport_freq[airport_freq.airport_ident == "KLAX"].sort_values("type")

# select * from airport_freq where airport_ident = 'KLAX' order by type desc	
airport_freq[airport_freq.airport_ident == "KLAX"].sort_values("type", ascending = False)

"""
--- IN… NOT IN
"""
# select id, ident, type from airports where type in ('heliport', 'balloonport')	
airports[airports.type.isin(["heliport", "balloonport"])][["id", "ident", "type"]]

# select * from airports where type not in ('heliport', 'balloonport')	
airports[~airports.type.isin(["heliport", "balloonport"])][["id", "ident", "type"]]


"""
--- GROUP BY, COUNT, ORDER BY
"""
# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, type	
airports.groupby([airports.iso_country, airports.type]).size()

# select iso_country, type, count(*) from airports group by iso_country, type order by iso_country, count(*) desc	
airports.groupby([airports.iso_country, airports.type]).size() \
    .reset_index(name="size") \
    .sort_values(["iso_country", "size"], ascending=[True, False])


"""
-- HAVING and WHERE
"""
# select type, count(*) from airports where iso_country = 'US' group by type having count(*) > 1000 order by count(*) desc	
airports[airports.iso_country == "US"].groupby(airports.type) \
    .filter(lambda g: len(g) > 1000).groupby(airports.type).size() \
    .reset_index(name="size").sort_values("size", ascending=False)


"""
-- Top N records
"""
# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10	
airports.groupby(airports.iso_country).size().reset_index(name="size") \
    .nlargest(10, "size")

# select iso_country, count(*) from airports group by iso_country order by count(*) desc limit 10 offset 10
airports.groupby(airports.iso_country).size().reset_index(name="size") \
    .nlargest(20, "size").tail(10)


"""
# -- Aggregate functions (MIN, MAX, AVG)
"""
# select min(elevation_ft), max(elevation_ft), avg(elevation_ft) from airports	
airports.agg({"elevation_ft": { "Max": np.max, "Min": np.min, "Avg": np.mean }}).T

"""
-- JOIN
"""
# select airport_ident, airport_freq.type, description, frequency_mhz from airport_freq join airports on airport_freq.airport_ref = airports.id where airports.ident = 'KLAX'	
airport_freq.merge(airports[airports.ident == "KLAX"], \
                   left_on = "airport_ref", right_on = "id", how = "inner") \
                   [["airport_ident", "type_x", "description", "frequency_mhz"]]

