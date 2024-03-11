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