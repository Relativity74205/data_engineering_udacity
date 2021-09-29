immigration = ("""
CREATE TABLE IF NOT EXISTS immigration (
immigration_id SERIAL PRIMARY KEY,
i94yr INT NOT NULL,
i94mon INT NOT NULL,
i94res INT NOT NULL REFERENCES i94_regions(region_id),
i94port VARCHAR(3) NOT NULL REFERENCES i94_cities(city_name_short),
i94mode INT NOT NULL REFERENCES i94_travel_modes(travel_mode_id),
i94addr VARCHAR(8) REFERENCES i94_states(state_name_short),
arrdate DATE NOT NULL,
depdate DATE,
i94bir INT NOT NULL,
i94visa INT NOT NULL REFERENCES i94_visa(visa_id),
gender VARCHAR(1),
airline VARCHAR(128)
);
""")

i94_regions = ("""
CREATE TABLE IF NOT EXISTS i94_regions (
region_id INT PRIMARY KEY,
region_name VARCHAR(256) NOT NULL
);
""")

i94_cities = ("""
CREATE TABLE IF NOT EXISTS i94_cities (
city_name_short VARCHAR(3) PRIMARY KEY,
city_name VARCHAR(128) NOT NULL,
us_state_name_short VARCHAR(2) REFERENCES i94_states(state_name_short),
country VARCHAR(64)
);
""")

i94_travel_modes = ("""
CREATE TABLE IF NOT EXISTS i94_travel_modes (
travel_mode_id INT PRIMARY KEY,
traval_mode VARCHAR(16) NOT NULL
);
""")

i94_states = ("""
CREATE TABLE IF NOT EXISTS i94_states (
state_name_short VARCHAR(2) PRIMARY KEY,
state_name VARCHAR(32) NOT NULL
);
""")

i94_visa = ("""
CREATE TABLE IF NOT EXISTS i94_visa (
visa_id INT PRIMARY KEY,
visa_name VARCHAR(16) NOT NULL
);
""")


ddl_queries = (i94_regions, i94_states, i94_cities, i94_travel_modes, i94_visa, immigration)
