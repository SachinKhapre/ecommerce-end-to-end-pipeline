import requests
import psycopg2
import logging
from psycopg2.extras import execute_values

#Logging
logging.basicConfig(level=logging.INFO)

DB_CONFIG = {
    'dbname' : 'fakestore-ecommerce',
    'user' : 'postgres',
    'password' : 'password',
    'host' : 'localhost',
    'port' : '5432'
}

#Endpoints
BASE_API = "https://fakestoreapi.com"
ENDPOINTS = {
    'products' : f'{BASE_API}/products',
    'users' : f'{BASE_API}/users',
    'orders' : f'{BASE_API}/carts'
}

def get_api_data(endpoint):
    response = requests.get(endpoint)
    response.raise_for_status()
    return response.json()

def insert_data(conn, data, columns, table):
    with conn.cursor() as cur:
        values = [[item.get(col, None) for col in columns] for item in data]
        insert_query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        execute_values(cur, insert_query, values)
        conn.commit()
        logging.info(f"Inserted {len(values)} records in {table}")

def tranform_user(user):
    return{
        'id' : user['id'],
        'email' : user['email'],
        'username' : user['username'],
        'city' : user['address']['city'],
        'zipcode' : user['address']['zipcode']
    }

def transform_order(order):
    return{
        'id' : order['id'],
        'user_id' : order['userId'],
        'date' : order['date']
    }

def transform_ordered_items(order):
    order_id = order['id']
    return [
        {
            'order_id' : order_id,
            'product_id' : item['productId'],
            'quantity' : item['quantity']
        }
    for item in order['products']
    ]

def main():
    conn = psycopg2.connect(**DB_CONFIG)

    #fetch and load products
    products = get_api_data(ENDPOINTS['products'])
    insert_data(conn, products, ['id', 'title', 'price', 'category', 'description'], 'products')

    #fetch and load users
    users = get_api_data(ENDPOINTS['users'])
    transformed_user = [tranform_user(user) for user in users]
    insert_data(conn, transformed_user, ['id', 'email', 'username', 'city', 'zipcode'], 'users')

    #fetch and load orders
    orders = get_api_data(ENDPOINTS['orders'])
    transformed_order = [transform_order(order) for order in orders]
    insert_data(conn, transformed_order, ['id', 'user_id', 'date'], 'orders')

    #load ordered items
    all_items = []
    for order in orders:
        all_items.extend(transform_ordered_items(order))
    insert_data(conn, all_items, ['order_id', 'product_id', 'quantity'], 'ordered_items')

    conn.close()
    logging.info('Ingestion Successfull')

if __name__ == '__main__':
    main()