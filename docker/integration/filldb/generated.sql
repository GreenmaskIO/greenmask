CREATE TABLE public.people
(
    id         integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    generated  text GENERATED ALWAYS AS (id || first_name) STORED,
    first_name text
);

INSERT INTO public.people("first_name")
VALUES ('bob');
