import psycopg
import mysql.connector
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

db1_name = os.getenv('DB1_NAME')
db1_user = os.getenv('DB1_USER')
db1_password = os.getenv('DB1_PASSWORD')
db1_host = os.getenv('DB1_HOST')
db1_port = os.getenv('DB1_PORT')

# Koneksi ke PostgreSQL
pg_conn = psycopg.connect(
    dbname=db1_name,  # Ganti dengan nama database PostgreSQL
    user=db1_user,       # Ganti dengan username PostgreSQL
    password=db1_password,  # Ganti dengan password PostgreSQL
    host=db1_host,  # Ganti dengan host PostgreSQL, jika diperlukan
    port=db1_port        # Port default PostgreSQL
)

pg_cursor = pg_conn.cursor()

db2_name = os.getenv('DB2_NAME')
db2_user = os.getenv('DB2_USER')
db2_password = os.getenv('DB2_PASSWORD')
db2_host = os.getenv('DB2_HOST')
db2_port = os.getenv('DB2_PORT')

# Koneksi ke PostgreSQL
pg_conn_dest = psycopg.connect(
    dbname=db2_name,  # Ganti dengan nama database PostgreSQL
    user=db2_user,       # Ganti dengan username PostgreSQL
    password=db2_password,  # Ganti dengan password PostgreSQL
    host=db2_host,  # Ganti dengan host PostgreSQL, jika diperlukan
    port=db2_port        # Port default PostgreSQL
)

pg_cursor_dest = pg_conn_dest.cursor()

# 1. Cek max(id) di MySQL untuk menentukan ID terakhir yang sudah ada
pg_cursor_dest.execute("SELECT MAX(id) FROM mqtt_data")
max_id_mysql = pg_cursor_dest.fetchone()[0]
if max_id_mysql is None:
    max_id_mysql = 0  # Jika tabel kosong, set id mulai dari 0

# 2. Ambil data dari PostgreSQL dengan ID yang lebih besar dari max_id_mysql
pg_cursor.execute("SELECT id, topic, payload, received_at FROM mqtt_data WHERE id > %s", (max_id_mysql,))
new_data = pg_cursor.fetchall()

# 3. Menyisipkan data yang baru ke MySQL
if new_data:
    insert_query = """
    INSERT INTO mqtt_data (id, topic, payload, received_at)
    VALUES (%s, %s, %s, %s)
    """
    pg_cursor_dest.executemany(insert_query, new_data)
    pg_conn_dest.commit()  # Commit perubahan ke MySQL
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {len(new_data)} baris data berhasil dipindahkan ke PostgresSQL.")
else:
    print("Tidak ada data baru yang perlu dipindahkan.")

# Menutup koneksi
pg_cursor.close()
pg_conn.close()
pg_cursor_dest.close()
pg_conn_dest.close()