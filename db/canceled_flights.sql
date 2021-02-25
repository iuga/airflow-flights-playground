SELECT 
    airline_code, flight_number, flight_date
FROM 
    flights
WHERE 
    flight_status = 'cancelled';