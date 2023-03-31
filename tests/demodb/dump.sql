create table public.flights
(
    id        INT PRIMARY KEY,
    flight_no TEXT        NOT NULL UNIQUE,
    departure TIMESTAMPTZ NOT NULL,
    arrival   TIMESTAMPTZ NOT NULL CHECK ( arrival > departure )
);


INSERT INTO public.flights (id, flight_no, departure, arrival)
SELECT generate_series, format('ABCD%s', generate_series), now() - INTERVAL '1 hour', now()
FROM generate_series(1, 100);
