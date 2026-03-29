

CREATE TABLE IF NOT EXISTS weather_raw ( 
	id 						SERIAL PRIMARY KEY,
	city 					VARCHAR(100) NOT NULL,
	country 				VARCHAR(10),
	temperature_c 			NUMERIC(5,2),
	feels_like_c 			NUMERIC(5,2),
	humidity_pct 			INTEGER,
	wind_speed_ms			NUMERIC(6,2),
	weather_desc 			VARCHAR(200),
	recorded_at 			TIMESTAMP	NOT NULL,
	ingested_at 			TIMESTAMP	DEFAULT NOW()
)