#!/usr/bin/env python
# coding: utf-8

# In[4]:


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


# In[5]:


### GOLD TABLE AND VISUALIZATION 1
import matplotlib.pyplot as plt
from collections import defaultdict

# Step 1: Aggregate data from Silver table
industry_counts = defaultdict(list)

rows = session.execute("SELECT industry, number_of_employees FROM silver_organizations")
for row in rows:
    if row.industry and row.number_of_employees:
        industry_counts[row.industry].append(row.number_of_employees)

# Step 2: Compute average number of employees per industry
avg_employees_per_industry = {
    industry: sum(emp_list) / len(emp_list)
    for industry, emp_list in industry_counts.items()
}

# Step 3: Insert into Gold table
insert_gold_query = """
INSERT INTO gold_industry_employees (industry, average_number_of_employees)
VALUES (%s, %s)
"""
for industry, avg in avg_employees_per_industry.items():
    session.execute(insert_gold_query, (industry, avg))

print("Data successfully inserted into Gold table.")

# Step 4: Fetch data from Gold for plotting
rows = session.execute("SELECT industry, average_number_of_employees FROM gold_industry_employees")

industries = []
avg_employees = []

for row in rows:
    industries.append(row.industry)
    avg_employees.append(row.average_number_of_employees)

# Sort and prepare data
sorted_data = sorted(zip(industries, avg_employees), key=lambda x: x[1], reverse=True)
industries, avgs = zip(*sorted_data)

# ✅ Optional: Only show top N industries for clarity
top_n = 20
industries = industries[:top_n]
avgs = avgs[:top_n]

# Step 5: Plot
plt.figure(figsize=(12, 8))  # Adjust height based on top_n
plt.barh(industries, avgs, color='skyblue')
plt.xlabel("Average Number of Employees")
plt.ylabel("Industry")
plt.title(f"Top {top_n} Industries by Average Number of Employees")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.show()


# In[ ]:




