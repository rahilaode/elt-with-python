CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CREATE SCHEMA FOR STAGING & FINAL(production)
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS prod AUTHORIZATION postgres;