-- Test table with VARCHAR column containing numeric-looking strings
-- This reproduces issue #394: leading zeros in GTINs being stripped

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gtin VARCHAR(14) NOT NULL,  -- GTIN-14 format, leading zeros are significant
    description TEXT
);

-- Insert test data with GTINs that have leading zeros
-- These are valid GTIN-14 identifiers where leading zeros are mandatory
INSERT INTO products (name, gtin, description) VALUES
    ('Product A', '00001402417161', 'GTIN with 4 leading zeros'),
    ('Product B', '00000012345678', 'GTIN with 6 leading zeros'),
    ('Product C', '01234567890123', 'GTIN with 1 leading zero'),
    ('Product D', '00100200300400', 'GTIN with mixed zeros'),
    ('Product E', '00000000000001', 'GTIN that is mostly zeros');

-- Also test with TEXT column
CREATE TABLE identifiers (
    id SERIAL PRIMARY KEY,
    code TEXT NOT NULL,
    type VARCHAR(50)
);

INSERT INTO identifiers (code, type) VALUES
    ('000123', 'numeric-looking text with leading zeros'),
    ('007', 'James Bond code'),
    ('00000001', 'Sequence number with padding');
