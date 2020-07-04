# Data dictionary

Data dictionary for the project, with the format `{schema}.{table}`.

---

`immigration.us_entries`

- `admnum`: Integer value, unique identifier of entry in the U.S.
- `i94bir`: Integer value, age of individual at time of entry
- `gender`: One letter for gender, non-binary
- `i94visa`: Numerical code representing type of entry in the U.S., converted with table `immigration.trip_reason_codes`
- `i94cit`: Numerical code for country of origin of the individual, converted with table `immigration.country_codes`
- `i94res`: Numerical code for country of residence of the individual, converted with table `immigration.country_codes`
- `i94mode`: Channel of arrival to the U.S., converted with table `immigration.entry_channel_codes`
- `arrival_day`: Integer, day of the month of arrival to the U.S., with NA encoded as -9999
- `arrival_month`: Integer, month of arrival to the U.S., with NA encoded as -9999
- `arrival_year`: Integer, year of arrival to the U.S., with NA encoded as -9999
- `departure_day`: Integer, day of the month of departure from the U.S., with NA encoded as -9999
- `departure_month`: Integer, month of departure from the U.S., with NA encoded as -9999
- `departure_year`: Integer, year of departure from the U.S., with NA encoded as -9999
- `length_of_stay`: Number of days in the U.S.

---

`immigration.country_codes`

- `code`: Numeric code
- `country_name`: Name of country

---

`immigration.port_codes`

- `code`: Numeric code
- `port_name`: Port of origin

---

`immigration.entry_channel_codes `

- `code`: Numeri code
- `entry_channel`: Channel of arrival in the U.S.

---

`immigration.state_codes`

- `code`: Alphanumerical code
- `state_name`: Name of the U.S. state

---

`immigration.trip_reason_codes`

- `code`: Numeric code
- `trip_reason`: Type of visa

---

`temperature.full_temperature_data`

- `dt `: Date of record, in the format YYYY-MM-DD
- `averagetemperature`: Average temperature recorded in the station for the day
- `averagetemperatureuncertainty`: Uncertainty over the mean value
- `city`: City where the station of measurement is located
- `country`: Country of measurement
- `latitude`: Latitude coordinate
- `longitud`: Longitude coordinate

---

`temperature.temp_summary`

- `country_name `: Name of the country
- `mean_temp `: Mean temperature recorded across all locations and dates available in the historical series of data
- `stddev_temp`: Standard deviation of all recorded temperatures across locations in the historical series of data

---

**IMPORTANT**: In the outputs schema, {month} equates to the three lower cased first digits of the month name in English (e.g. jan, feb...) while {year} is represented by all 4 digits

---

`outputs.{month}{year}_demographics_by_channel`

- `entry_channel`: String representing the entry channel
- `gender`: One letter for gender, non-binary
- `average_age`: Average age of the segment in the month of arrivals

---

`outputs.{month}{year}_length_of_stay`

- `country_name`: Name of the country
- `average_stay`: Average stay of individuals arriving to the U.S. in the month
---

`outputs.{month}{year}_state_trip_reasons`

- `state_name`: Name of the U.S. state listed as destination
- `trip_reason`: Reason for the trip based on visa status
- `count`: Number of arrivals for the segment in the month

---

`outputs.{month}{year}_freqs_and_mean_temps`

- `country_name`: Name of the country
- `visitor_count`: Number of visitors in from the country in the month	
- `mean_temp`: Mean temperature of country of origin
- `stddev_temp`: Standard deviation of the series of historical temperatures for the country 


