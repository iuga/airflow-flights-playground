/* Initalization Scripts */

SET GLOBAL local_infile=1;

SHOW GLOBAL VARIABLES LIKE 'local_infile';

DROP TABLE flights;
CREATE TABLE IF NOT EXISTS flights (
     airline_code VARCHAR(3) NOT NULL,
     flight_number  VARCHAR(256) NOT NULL,
     flight_date  DATE NOT NULL,
     flight_status  VARCHAR(12) NOT NULL,
     departure_airport  VARCHAR(256) DEFAULT NULL,
     arrival_airport  VARCHAR(256) DEFAULT NULL,
     airline_name VARCHAR(256) DEFAULT NULL,
     insert_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
     PRIMARY KEY (airline_code, flight_number, flight_date)
);