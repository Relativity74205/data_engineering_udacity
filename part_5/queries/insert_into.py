immigration = ("""
INSERT INTO immigration 
(i94yr, i94mon, i94res, i94port, i94mode, i94addr, arrdate, depdate, i94bir, i94visa, gender, airline)
VALUES %s
""")


i94_regions = ("""
INSERT INTO i94_regions 
(region_id, region_name)
VALUES (%s, %s)
""")

i94_ports = ("""
INSERT INTO i94_cities 
(city_name_short, city_name, us_state_name_short, country)
VALUES (%s, %s, %s, %s)
""")

i94_travel_modes = ("""
INSERT INTO i94_travel_modes 
(travel_mode_id, traval_mode)
VALUES (%s, %s)
""")

i94_states = ("""
INSERT INTO i94_states 
(state_name_short, state_name)
VALUES (%s, %s)
""")

i94_visa = ("""
INSERT INTO i94_visa 
(visa_id, visa_name)
VALUES (%s, %s)
""")
