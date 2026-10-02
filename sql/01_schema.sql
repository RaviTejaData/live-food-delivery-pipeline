CREATE TABLE customers (
    customer_id   SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    city          TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE restaurants (
    restaurant_id SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    cuisine       TEXT NOT NULL,
    city          TEXT NOT NULL
);

CREATE TABLE riders (
    rider_id      SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    vehicle       TEXT NOT NULL,
    is_available  BOOLEAN DEFAULT true
);

CREATE TABLE orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INT NOT NULL REFERENCES customers(customer_id),
    restaurant_id INT NOT NULL REFERENCES restaurants(restaurant_id),
    rider_id      INT REFERENCES riders(rider_id),
    status        TEXT NOT NULL DEFAULT 'placed',
    total_amount  NUMERIC(10,2) NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT now(),
    updated_at    TIMESTAMPTZ DEFAULT now(),
    CHECK (status IN ('placed','cooking','on_the_way','delivered','cancelled'))
);

CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INT NOT NULL REFERENCES orders(order_id),
    item_name     TEXT NOT NULL,
    quantity      INT NOT NULL CHECK (quantity > 0),
    unit_price    NUMERIC(8,2) NOT NULL
);

CREATE INDEX idx_orders_status ON orders(status);
