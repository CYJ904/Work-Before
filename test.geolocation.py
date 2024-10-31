import sqlite3

# Connect to the database (replace 'your_database.db' with the actual database file)
conn = sqlite3.connect('olist.sqlite')
cursor = conn.cursor()

# Query to find review_id with more than one occurrence
query = """
SELECT geolocation_zip_code_prefix, COUNT(geolocation_lat)
FROM geolocation
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(geolocation_lat) > 1;
"""

# Execute the query
cursor.execute(query)
results = cursor.fetchall()

# Check if any review_id appears more than once
if results:
    print("The following review_ids appear more than once:")
    idx = 1
    for review_id, count in results:
        print(f"number {idx}, geolocation_zip_code_prefix: {review_id}, count: {count}")
        idx += 1
else:
    print("All review_ids are unique.")

# Close the database connection
conn.close()

