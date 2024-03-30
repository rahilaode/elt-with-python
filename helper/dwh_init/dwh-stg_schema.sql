CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION postgres;

-- Staging
CREATE TABLE stg.category (
    id uuid default uuid_generate_v4(),
    category_id serial primary key NOT NULL,
    "name" varchar(255) NOT NULL,
    description text NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.customer (
    id uuid default uuid_generate_v4(),
    customer_id serial primary key NOT NULL,
    first_name varchar(255) NOT NULL,
    last_name varchar(255) NOT NULL,
    email varchar(255) NOT NULL,
    phone varchar(100) NULL,
    address text NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.orders (
    id uuid default uuid_generate_v4(),
    order_id varchar(50) primary key NOT NULL,
    customer_id int4 NULL,
    order_date date NOT NULL,
    status varchar(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.order_detail (
    id uuid default uuid_generate_v4(),
    order_detail_id serial primary key NOT NULL,
    order_id varchar(50) NULL,
    product_id varchar(100) NULL,
    quantity int4 NOT NULL,
    price numeric(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE stg.subcategory (
    id uuid default uuid_generate_v4(),
	subcategory_id serial primary key NOT NULL,
	"name" varchar(255) NOT NULL,
	category_id int4 NULL,
	description text NULL,
	created_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamp DEFAULT CURRENT_TIMESTAMP NULL
);

CREATE TABLE stg.product (
    id uuid default uuid_generate_v4(),
    product_id varchar(100) primary key NOT NULL,
    "name" text NOT NULL,
    subcategory_id int4 NULL,
    price numeric(10, 2) NOT NULL,
    stock int4 NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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