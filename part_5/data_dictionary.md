# Data dictionary

### table _immigration_

- star schema fact table
- primary key: _immigration_id_

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| immigration_id | integer   | custom id; primary key | auto generated |
| i94yr          | integer   | year of the immigration | immigration data |
| i94mon         | integer   | month of the immigration | immigration data |
| i94res         | integer   | code of the country of residence; references the _i94_regions_ dimension table  | immigration data |
| i94port        | varchar   | code for the immigration port; references the _i94_ports_ dimension table | immigration data  |
| i94mode        | integer   | code for the travel mode; references the _i94_travel_modes_ dimension table | immigration data  |
| i94addr        | varchar   | code for the immigration state; references the _i94_states_ dimension table | immigration data  |
| arrdate        | date      | arrival date | immigration data |
| depdate        | date      | departure date | immigration data |
| i94bir         | integer   | age | immigration data |
| i94visa        | integer   | code for visa type; references the _i94_visa_ dimension table | immigration data |
| gender         | varchar   | gender | immigration data |
| airline        | varchar   | airline code | immigration data |


### table _i94_regions_

- star schema dimensional table
- primary key: _region_id_

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| region_id      | integer   | code of the country of residence | immigration data description file |
| region_name    | varchar   | name of the country of residence | immigration data description file |


### table _i94_ports_

- star schema dimensional table
- primary key: _port_code_

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| port_code      | varchar   | code of the immigration port | immigration data description file |
| city           | varchar   | city of the immigration port | immigration data description file |
| state_code     | varchar   | code of the immigration state | immigration data description file |
| country        | varchar   | country of the immigration | immigration data description file |


### table _i94_travel_modes_

- star schema dimensional table
- primary key: _travel_mode_id_

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| travel_mode_id | integer   | code for the travel mode | immigration data description file |
| travel_mode    | varchar   | name of the travel mode | immigration data description file |


### table _i94_states_

- star schema dimensional table
- primary key: _travel_mode_id_

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| state_code     | VARCHAR   | code of the state | immigration data description file |
| state          | varchar   | name of the state | immigration data description file |
| white          | integer   | state population for this race | demographics on state level |
| black          | integer   | state population for this race | demographics on state level |
| hispanic       | integer   | state population for this race | demographics on state level |
| asian          | integer   | state population for this race | demographics on state level |
| american_indian_alaska_native | integer   | state population for this race | demographics on state level |
| native_hawaiian_other_pacific_islander | integer   | state population for this race | demographics on state level |
| multiple_races | integer   | state population for this race | demographics on state level |
| total          | integer   | total state population | demographics on state level |


### table _i94_visa_

- star schema dimensional table
- primary key: _visa_id_

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| visa_id        | integer   | code for the visa type | immigration data description file |
| visa_name      | varchar   | name of the visa type | immigration data description file |


### table _demographics_

- star schema dimensional table
- primary key: _city_, _state_code_

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| city           | varchar   | city name | demographics on city level |
| state_code     | varchar   | name of the visa type | demographics on city level |
| total_population | integer   | total city population | demographics on city level |
| foreign_born | integer   | foreign born city population | demographics on city level |
| american_indian_and_alaska_native | integer   | city population for this race | demographics on city level |
| asian | integer   | city population for this race | demographics on city level |
| black_or_african_american | integer   | city population for this race | demographics on city level |
| hispanic_or_latino | integer   | city population for this race | demographics on city level |
| hispanic_or_latino | integer   | city population for this race | demographics on city level |


### table _temperatures_

- star schema dimensional table
- primary key: (_city_, _country_, _latitude_, _longitude_, _calendar_month_)

| col_name       | col_type  | explanation | origin |
| ---            | ---       | ---         | ---    |
| city           | integer   | city name | temperature data |
| country        | varchar   | country name | temperature data |
| latitude       | varchar   | latitude gps coordinates | temperature data |
| longitude      | varchar   | longitude gps coordinates | temperature data |
| calendar_month | integer   | calendar month as integer | temperature data |
| mean_temperature | float   | mean city temperature for this calendar month of the last three years | temperature data |
