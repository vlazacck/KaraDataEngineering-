import psycopg2

# Database connection parameters
conn = psycopg2.connect(
    dbname="ethiopian_medical_data",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()