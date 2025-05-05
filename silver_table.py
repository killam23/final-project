#!/usr/bin/env python
# coding: utf-8

# In[2]:


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


# In[3]:


## SILVER TABLE 
from cassandra.query import BatchStatement

# Prepare insert query
insert_silver_query = session.prepare("""
INSERT INTO silver_organizations (
    org_index, organization_id, name, website, country,
    description, founded, industry, number_of_employees
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Batch insert setup
batch_size = 30
batch = BatchStatement()
count = 0

# Read from Bronze and insert into Silver
bronze_rows = session.execute("SELECT * FROM bronze_organizations")
for row in bronze_rows:
    try:
        if all([
            row.organization_id,
            row.name,
            row.country,
            row.founded is not None,
            row.industry,
            row.number_of_employees is not None
        ]):
            batch.add(insert_silver_query, (
                row.org_index,
                row.organization_id,
                row.name,
                row.website,
                row.country,
                row.description,
                row.founded,
                row.industry,
                row.number_of_employees
            ))
            count += 1

            if count % batch_size == 0:
                session.execute(batch)
                batch.clear()
    except:
        continue  # Ignore bad rows silently

# Final flush for any remaining rows
if batch:
    session.execute(batch)

print("Successfully all cleaned rows are inserted into silver table")


# In[ ]:




