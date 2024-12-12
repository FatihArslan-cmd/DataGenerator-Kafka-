#!/usr/bin/python3

import psycopg2  # pip install psycopg2
from kafka import KafkaProducer  # pip install kafka-python
import numpy as np  # pip install numpy
from time import time, sleep
import random
import json

DB_CONFIG = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'fatih',
    'host': 'localhost',
    'port': 5432
}

KAFKA_SERVER = 'localhost:9092'

# Expense categories
EXPENSE_DESCRIPTIONS = [
    "Macaroni", "Jacket", "Car", "Coffee",
    "Laptop", "Book", "Chair", "Bike"
]

def get_user_ids():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT empno FROM emp")
    user_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return user_ids

# Generate a random expense record
def generate_expense(user_id):
    description = random.choice(EXPENSE_DESCRIPTIONS)
    payment = round(np.random.uniform(5, 500), 2)  # Random amount between $5 and $500
    return {
        "user_id": user_id,
        "date_time": time(),
        "description": description,
        "payment": payment
    }

def main():
    user_ids = get_user_ids()
    if not user_ids:
        print("No users found in the database.")
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Generating expenses for users: {user_ids}")

    try:
        count = 1
        while True:
            for user_id in user_ids:
                expense = generate_expense(user_id)
                topic_name = f"user_{user_id}_expenses"
                producer.send(topic_name, expense)
                print(f"Sent to Kafka (#{count}): {expense}")
                count += 1
                sleep(1)
    except KeyboardInterrupt:
        print("\nStopping data generator.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
