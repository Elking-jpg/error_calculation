
CREATE TABLE historical_hourly_weather (
    weather_timestamp TIMESTAMP WITH TIME ZONE,
    city VARCHAR(100),
    actual_temperature NUMERIC(4,2),
    PRIMARY KEY (weather_timestamp, city)
);


CREATE TABLE hourly_forecasts (
    execution_timestamp TIMESTAMP WITH TIME ZONE,
    forecast_timestamp TIMESTAMP WITH TIME ZONE,
    city VARCHAR(100),
    predicted_temperature NUMERIC(4,2),
    lead_time_hours INT,
    PRIMARY KEY (execution_timestamp, forecast_timestamp, city)
);