-- let's create a TABLE
CREATE TABLE IF NOT EXISTS realestate(
    bathrooms NUMERIC,
    bedrooms NUMERIC,
    city VARCHAR(255),
    homeStatus VARCHAR(255),
    homeType VARCHAR(255),
    livingArea NUMERIC,
    price NUMERIC,
    rentZestimate NUMERIC,
    zipcode INT
);

-- let's select some data (nothing currently)
SELECT * FROM realestate;