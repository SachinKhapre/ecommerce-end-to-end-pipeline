-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    email TEXT,
    username TEXT,
    city TEXT,
    zipcode TEXT
);

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY,
    title TEXT,
    price NUMERIC,
    category TEXT,
    description TEXT
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY,
    user_id INT,
    date TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Create ordered_items table
CREATE TABLE IF NOT EXISTS ordered_items (
    order_id INT,
    product_id INT,
    quantity INT,
    PRIMARY KEY (order_id, product_id),
    FOREIGN KEY (order_id) REFERENCES orders(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
)