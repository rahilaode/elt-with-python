CREATE TABLE stg.category (
    category_id serial4 NOT NULL,
    "name" varchar(255) NOT NULL,
    description text NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT category_pkey PRIMARY KEY (category_id)
);

CREATE TABLE stg.customer (
    customer_id serial4 NOT NULL,
    first_name varchar(255) NOT NULL,
    last_name varchar(255) NOT NULL,
    email varchar(255) NOT NULL,
    phone varchar(100) NULL,
    address text NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT customer_pkey PRIMARY KEY (customer_id)
);

CREATE TABLE stg.orders (
    order_id varchar(50) NOT NULL,
    customer_id int4 NULL,
    order_date timestamp NOT NULL,
    status varchar(50) NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT orders_pkey PRIMARY KEY (order_id)
);

CREATE TABLE stg.order_detail (
    order_detail_id serial4 NOT NULL,
    order_id varchar(50) NULL,
    product_id varchar(100) NULL,
    quantity int4 NOT NULL,
    price numeric(10, 2) NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT order_detail_pkey PRIMARY KEY (order_detail_id)
);

CREATE TABLE stg.subcategory (
	subcategory_id serial4 NOT NULL,
	"name" varchar(255) NOT NULL,
	category_id int4 NULL,
	description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT subcategory_pkey PRIMARY KEY (subcategory_id)
);

CREATE TABLE stg.product (
    product_id varchar(100) NOT NULL,
    "name" text NOT NULL,
    subcategory_id int4 NULL,
    price numeric(10, 2) NOT NULL,
    stock int4 NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
    CONSTRAINT product_pkey PRIMARY KEY (product_id)
);

ALTER TABLE stg.orders
ADD FOREIGN KEY (customer_id) REFERENCES stg.customer(customer_id);

ALTER TABLE stg.order_detail
ADD FOREIGN KEY (order_id) REFERENCES stg.orders(order_id);

ALTER TABLE stg.order_detail
ADD FOREIGN KEY (product_id) REFERENCES stg.product(product_id);

ALTER TABLE stg.subcategory
ADD FOREIGN KEY (category_id) REFERENCES stg.category(category_id);

ALTER TABLE stg.product
ADD FOREIGN KEY (subcategory_id) REFERENCES stg.subcategory(subcategory_id);

CREATE TABLE stg.dim_customer(
    customer_id uuid primary key default uuid_generate_v4(),
    customer_nk int,
    first_name varchar(100),
    last_name varchar(100),
    email varchar(100),
    phone varchar(100),
    address varchar(100),
    created_at timestamp,
    updated_at timestamp
);

CREATE TABLE stg.dim_product(
    product_id uuid primary key default uuid_generate_v4(),
    product_nk int,
    name varchar(100),
    price numeric(10,2),
    stock int,
    category_name varchar(100),
    category_desc text,
    subcategory_name varchar(100),
    subcategory_desc text,
    created_at timestamp,
    updated_at timestamp
);

CREATE TABLE stg.dim_date(
    date_id              	 INT NOT null primary KEY,
    date_actual              DATE NOT NULL,
    day_suffix               VARCHAR(4) NOT NULL,
    day_name                 VARCHAR(9) NOT NULL,
    day_of_year              INT NOT NULL,
    week_of_month            INT NOT NULL,
    week_of_year             INT NOT NULL,
    week_of_year_iso         CHAR(10) NOT NULL,
    month_actual             INT NOT NULL,
    month_name               VARCHAR(9) NOT NULL,
    month_name_abbreviated   CHAR(3) NOT NULL,
    quarter_actual           INT NOT NULL,
    quarter_name             VARCHAR(9) NOT NULL,
    year_actual              INT NOT NULL,
    first_day_of_week        DATE NOT NULL,
    last_day_of_week         DATE NOT NULL,
    first_day_of_month       DATE NOT NULL,
    last_day_of_month        DATE NOT NULL,
    first_day_of_quarter     DATE NOT NULL,
    last_day_of_quarter      DATE NOT NULL,
    first_day_of_year        DATE NOT NULL,
    last_day_of_year         DATE NOT NULL,
    mmyyyy                   CHAR(6) NOT NULL,
    mmddyyyy                 CHAR(10) NOT NULL,
    weekend_indr             VARCHAR(20) NOT NULL
);

CREATE TABLE stg.fact_order(
	order_id uuid primary key default uuid_generate_v4() ,
	product_id uuid references stg.dim_product(product_id),
	customer_id uuid references stg.dim_customer(customer_id),
	order_date int references stg.dim_date(date_id),
	quantity int,
	status varchar(50),
	created_at timestamp,
	updated_at timestamp
);

-- Populating for staging date dimension 
INSERT INTO stg.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;