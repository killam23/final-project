#!/usr/bin/env python
# coding: utf-8

# In[1]:


from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Secure connect bundle path (update to your real path)
SECURE_CONNECT_BUNDLE_PATH = r'C:\Users\DELL\secure-connect-finalproject.zip'

# Astra DB credentials (replace with valid values)
ASTRA_DB_CLIENT_ID = 'OHcWaOTFhHqZggUSlhIZUgIt'
ASTRA_DB_CLIENT_SECRET = 'unLZx3Nk+BL0naBfSn_QvSUFHwmEddiF3NiG.EGSwdrwnvc65+,2-MS,,xhirlTDXXkfKAwKUzyAM9JUsNn5T6sAzbertMXMBDe.A.35RUErsaZwTeroJPdBvzXb,XMT'
ASTRA_DB_KEYSPACE = 'project'

# Connect to Astra DB
cloud_config = {'secure_connect_bundle': SECURE_CONNECT_BUNDLE_PATH}
auth_provider = PlainTextAuthProvider(ASTRA_DB_CLIENT_ID, ASTRA_DB_CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect(ASTRA_DB_KEYSPACE)

print("✅ Connected to Cassandra.")


# In[2]:


## BRONZE TABLE
import os
import csv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement

# Optional: verify driver installed
import cassandra
print("Cassandra driver is installed and ready!")

# Set working directory
os.chdir(r'C:\Users\DELL')
print("Current directory:", os.getcwd())

# Path to your secure connect bundle
ASTRA_DB_BUNDLE_PATH = 'secure-connect-finalproject.zip'

# Create Cassandra session
def create_session():
    cloud_config = {
        'secure_connect_bundle': ASTRA_DB_BUNDLE_PATH
    }
    auth_provider = PlainTextAuthProvider(
        'OHcWaOTFhHqZggUSlhIZUgIt',
        'unLZx3Nk+BL0naBfSn_QvSUFHwmEddiF3NiG.EGSwdrwnvc65+,2-MS,,xhirlTDXXkfKAwKUzyAM9JUsNn5T6sAzbertMXMBDe.A.35RUErsaZwTeroJPdBvzXb,XMT'
    )
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace('project')
    return session

session = create_session()

# Prepare insert query
insert_query = session.prepare("""
INSERT INTO bronze_organizations (
    org_index, organization_id, name, website, country,
    description, founded, industry, number_of_employees
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Batch insert settings
batch_size = 30
batch = BatchStatement()
count = 0

# Read CSV and insert all rows
with open('organizations-100000.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for i, row in enumerate(reader):
        try:
            batch.add(insert_query, (
                int(row['Index']),
                row['Organization Id'],
                row['Name'],
                row['Website'],
                row['Country'],
                row['Description'],
                int(row['Founded']) if row['Founded'] else None,
                row['Industry'],
                int(row['Number of employees']) if row['Number of employees'] else None
            ))
            count += 1

            if count % batch_size == 0:
                session.execute(batch)
                batch.clear()

            if count % 1000 == 0:
                print(f"Inserted {count} rows...")

        except Exception as e:
            print(f"Error at row {i}: {e}")

# Insert remaining rows
if batch:
    session.execute(batch)
    print(f"Inserted remaining {count % batch_size} rows.")

print(f"\n✅ Done! Total rows inserted: {count}")

# Query and show a few rows
print("\n🔍 Sample data:")
rows = session.execute("SELECT * FROM bronze_organizations LIMIT 5")
for row in rows:
    print(row)


# In[ ]:




