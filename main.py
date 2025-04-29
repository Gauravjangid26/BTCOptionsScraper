import requests
import sqlite3
import json

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

# Step 3: Create SQLite database and table
def setup_database():
    conn = sqlite3.connect("deribit_options.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS options (
            instrument_name TEXT,
            strike REAL,
            expiration_timestamp INTEGER,
            option_type TEXT,
            min_trade_amount REAL
        )
    """)
    conn.commit()
    return conn, cursor

# Step 4: Insert filtered data into database
def insert_data(cursor, instruments):
    for item in instruments:
        cursor.execute("""
            INSERT INTO options (instrument_name, strike, expiration_timestamp, option_type, min_trade_amount)
            VALUES (?, ?, ?, ?, ?)
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
    print("[4] Setting up SQLite database...")
    conn, cursor = setup_database()
    
     print("[5] Inserting data into the database...")
    insert_data(cursor, filtered)

    conn.commit()
    conn.close()

    print(" Data saved in 'deribit_options.db'.")

if __name__ == "__main__":
    main()
