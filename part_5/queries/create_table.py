immigration = ("""
CREATE TABLE IF NOT EXISTS immigration (
immigration_id SERIAL PRIMARY KEY,
i94yr INT NOT NULL,
i94mon INT NOT NULL,
i94res INT NOT NULL REFERENCES i94_regions(region_id),
i94port VARCHAR(3) NOT NULL REFERENCES i94_ports(port_code),
i94mode INT NOT NULL REFERENCES i94_travel_modes(travel_mode_id),
i94addr VARCHAR(8) REFERENCES i94_states(state_code),
arrdate DATE NOT NULL,
depdate DATE,
i94bir INT NOT NULL,
i94visa INT NOT NULL REFERENCES i94_visa(visa_id),
gender VARCHAR(8),
airline VARCHAR(128)
);
""")

i94_regions = ("""
CREATE TABLE IF NOT EXISTS i94_regions (
region_id INT PRIMARY KEY,
region_name VARCHAR(256) NOT NULL
);
""")

i94_ports = ("""
CREATE TABLE IF NOT EXISTS i94_ports (
port_code VARCHAR(3) PRIMARY KEY,
port VARCHAR(64) NOT NULL,
state_code VARCHAR(2) REFERENCES i94_states(state_code),
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
state_code VARCHAR(2) PRIMARY KEY,
state VARCHAR(32) NOT NULL,
white INT,
black INT,
hispanic INT,
asian INT,
american_indian_alaska_native INT,
native_hawaiian_other_pacific_islander INT,
multiple_races INT,
total INT
);
""")

i94_visa = ("""
CREATE TABLE IF NOT EXISTS i94_visa (
visa_id INT PRIMARY KEY,
visa_name VARCHAR(16) NOT NULL
);
""")

demographics = ("""
CREATE TABLE IF NOT EXISTS demographics (
city VARCHAR(64) NOT NULL,
state_code VARCHAR(2) NOT NULL,
total_population INT NOT NULL,
foreign_born INT,
american_indian_and_alaska_native INT NOT NULL,
asian INT NOT NULL,
black_or_african_american INT NOT NULL,
hispanic_or_latino INT NOT NULL,
white INT NOT NULL,
PRIMARY KEY(city, state_code) 
);
""")

temperatures = ("""
CREATE TABLE IF NOT EXISTS temperatures(
city VARCHAR(64) NOT NULL,
country VARCHAR(64) NOT NULL,
latitude varchar(8) NOT NULL,
longitude varchar(8) NOT NULL,
calendar_month INT NOT NULL,
mean_temperature NUMERIC NOT NULL,
PRIMARY KEY(city, country, latitude, longitude, calendar_month)
); 
""")

ddl_queries = (demographics, temperatures, i94_regions, i94_states, i94_ports, i94_travel_modes, i94_visa, immigration, )
