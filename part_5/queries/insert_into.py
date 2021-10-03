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
INSERT INTO i94_ports 
(port_code, port, state_code, country)
VALUES (%s, %s, %s, %s)
""")

i94_travel_modes = ("""
INSERT INTO i94_travel_modes 
(travel_mode_id, traval_mode)
VALUES (%s, %s)
""")

i94_states = ("""
INSERT INTO i94_states 
(state_code, state, white, black, hispanic, asian, american_indian_alaska_native, native_hawaiian_other_pacific_islander, multiple_races, total)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

i94_visa = ("""
INSERT INTO i94_visa 
(visa_id, visa_name)
VALUES (%s, %s)
""")

temperatures = ("""
INSERT INTO temperatures 
(city, country, latitude, longitude, calendar_month, mean_temperature)
VALUES %s
""")

demographics = ("""
INSERT INTO demographics 
(city, state_code, total_population, foreign_born, american_indian_and_alaska_native, asian, black_or_african_american, hispanic_or_latino, white)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")
