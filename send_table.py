import psycopg
import os
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection(db_name, db_user, db_password, db_host, db_port):
    """Establish a connection to a PostgreSQL database."""
    return psycopg.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
    )

def adapt_data(row):
    """Convert dictionary fields to JSON strings for psycopg."""
    return tuple(json.dumps(value) if isinstance(value, dict) else value for value in row)

def migrate_table(source_conn, dest_conn, table_name):
    """Migrate data from source to destination for a specific table."""
    source_cursor = source_conn.cursor()
    dest_cursor = dest_conn.cursor()

    # Get the max ID from the destination database
    dest_cursor.execute(f"SELECT MAX(id) FROM {table_name}")
    max_id = dest_cursor.fetchone()[0] or 0

    # Fetch new data from the source database
    source_cursor.execute(f"SELECT * FROM {table_name} WHERE id > %s", (max_id,))
    new_data = source_cursor.fetchall()

    if new_data:
        # Generate insert query dynamically
        column_names = [desc[0] for desc in source_cursor.description]
        columns = ', '.join(column_names)
        placeholders = ', '.join(['%s'] * len(column_names))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        # Convert data before insertion
        adapted_data = [adapt_data(row) for row in new_data]

        # Insert new data
        dest_cursor.executemany(insert_query, adapted_data)
        dest_conn.commit()

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {len(new_data)} rows migrated from {table_name}.")
    else:
        print(f"No new data to migrate for {table_name}.")

    # Close cursors
    source_cursor.close()
    dest_cursor.close()

if __name__ == "__main__":
    # Source DB credentials
    source_conn = get_db_connection(
        os.getenv('DB1_NAME'),
        os.getenv('DB1_USER'),
        os.getenv('DB1_PASSWORD'),
        os.getenv('DB1_HOST'),
        os.getenv('DB1_PORT')
    )

    # Destination DB credentials
    dest_conn = get_db_connection(
        os.getenv('DB2_NAME'),
        os.getenv('DB2_USER'),
        os.getenv('DB2_PASSWORD'),
        os.getenv('DB2_HOST'),
        os.getenv('DB2_PORT')
    )

    # List of tables to migrate
    tables_to_migrate = ['mqtt_data', 'movie_xx1_data']

    for table in tables_to_migrate:
        migrate_table(source_conn, dest_conn, table)

    # Close connections
    source_conn.close()
    dest_conn.close()
