import requests
import mysql.connector
from mysql.connector import Error

# Step 1: Fetch all BTC option instruments from Deribit testnet
def fetch_instruments():
    url = "https://test.deribit.com/api/v2/public/get_instruments"
    params = {
        "currency": "BTC",
        "kind": "option"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise error if status != 200
    return response.json()["result"]

# Step 2: Filter instruments with one common expiration timestamp
def filter_by_expiry(instruments):
    if not instruments:
        return []

    selected_expiry = instruments[0]["expiration_timestamp"]
    return [i for i in instruments if i["expiration_timestamp"] == selected_expiry]

# Step 3: Connect to MySQL database
def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host="localhost",  # Typically localhost for XAMPP MySQL
            user="root",       # Default XAMPP MySQL user
            password="",       # Default XAMPP MySQL password (empty)
            database="deribit_db"  # The name of your database
        )
        if connection.is_connected():
            print("Connected to MySQL database")
        return connection
    except Error as e:
        print(f"Error: {e}")
        return None

# Step 4: Create table in MySQL (if not exists)
def setup_mysql_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS options (
            instrument_name VARCHAR(255),
            strike REAL,
            expiration_timestamp BIGINT,
            option_type VARCHAR(50),
            min_trade_amount REAL
        )
    """)

# Step 5: Insert filtered data into MySQL database
def insert_data(cursor, instruments):
    for item in instruments:
        cursor.execute("""
            INSERT INTO options (instrument_name, strike, expiration_timestamp, option_type, min_trade_amount)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            item["instrument_name"],
            item["strike"],
            item["expiration_timestamp"],
            item["option_type"],
            item["min_trade_amount"]
        ))

# Main function
def main():
    print("[1] Fetching instruments from Deribit testnet...")
    all_instruments = fetch_instruments()

    print("[2] Filtering by one expiry date...")
    filtered = filter_by_expiry(all_instruments)

    print(f"[3] Total filtered instruments: {len(filtered)}")

    print("[4] Connecting to MySQL database...")
    connection = connect_to_mysql()

    if connection:
        cursor = connection.cursor()

        print("[5] Setting up MySQL table...")
        setup_mysql_table(cursor)

        print("[6] Inserting data into MySQL database...")
        insert_data(cursor, filtered)

        connection.commit()  # Commit changes
        cursor.close()
        connection.close()

        print("[✅] Done. Data saved in MySQL database.")
    else:
        print("Failed to connect to MySQL database.")

if __name__ == "__main__":
    main()
