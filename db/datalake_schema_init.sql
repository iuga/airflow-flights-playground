/* Initalization Scripts for AviationStack Data */

CREATE TABLE IF NOT EXISTS raw_active_flights (
     id SERIAL PRIMARY KEY,
     flight_date  DATE NOT NULL,
     flight_status  varchar(6) NOT NULL,
     airline_name  varchar(256) DEFAULT NULL,
     flight_number  varchar(256) DEFAULT NULL,
     departure_airport  varchar(256) DEFAULT NULL,
     departure_timezone  varchar(256) DEFAULT NULL,
     arrival_airport  varchar(256) DEFAULT NULL,
     arrival_timezone  varchar(256) DEFAULT NULL,
     arrival_terminal  varchar(256) DEFAULT NULL,
     insert_timestamp  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);