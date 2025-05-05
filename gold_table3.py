#!/usr/bin/env python
# coding: utf-8

# In[5]:


## GOLD TABLE(VISULAIZATION 3)
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
import matplotlib.pyplot as plt
import os

# Step 1: Connect to Cassandra Cluster
def create_session():
    try:
        bundle_path = 'secure-connect-finalproject.zip'
        if not os.path.exists(bundle_path):
            raise FileNotFoundError(f"Secure connect bundle not found at: {bundle_path}")

        cloud_config = {'secure_connect_bundle': bundle_path}
        auth_provider = PlainTextAuthProvider(
            'OHcWaOTFhHqZggUSlhIZUgIt',
            'unLZx3Nk+BL0naBfSn_QvSUFHwmEddiF3NiG.EGSwdrwnvc65+,2-MS,,xhirlTDXXkfKAwKUzyAM9JUsNn5T6sAzbertMXMBDe.A.35RUErsaZwTeroJPdBvzXb,XMT'
        )
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace('project')
        print("✅ Connected to Cassandra successfully.")
        return session
    except Exception as e:
        print("❌ Error connecting to Cassandra:", e)
        return None

# Step 2–4: Main logic
def process_and_insert_data(session):
    try:
        rows = session.execute("SELECT founded, number_of_employees FROM silver_organizations")

        data = []
        for row in rows:
            if row.founded is not None and row.number_of_employees is not None:
                data.append((row.founded, row.number_of_employees))

        if not data:
            print("⚠️ No valid data retrieved from silver_organizations.")
            return

        print(f"📊 Retrieved {len(data)} rows. Sample:")
        for d in data[:5]:
            print(d)

        # Step 3: Plot
        founded_years = [d[0] for d in data]
        num_employees = [d[1] for d in data]

        plt.figure(figsize=(10, 6))
        plt.scatter(founded_years, num_employees, alpha=0.3)
        plt.xlabel("Founded Year")
        plt.ylabel("Number of Employees")
        plt.title("Company Size vs. Founded Year")
        plt.grid(True)
        plt.tight_layout()

        output_file = "scatter_plot.png"
        plt.savefig(output_file)
        print(f"📌 Plot saved to: {output_file}")

        # Step 4: Batch insert into gold table
        print("📝 Inserting data into gold_founded_vs_employees...")

        insert_query = session.prepare("""
            INSERT INTO gold_founded_vs_employees (founded_year, number_of_employees)
            VALUES (?, ?)
        """)

        batch = BatchStatement()
        batch_size = 30
        count = 0
        inserted = 0

        for year, employees in data:
            batch.add(insert_query, (year, employees))
            count += 1

            if count % batch_size == 0:
                session.execute(batch)
                inserted += len(batch)
                batch.clear()

        # Flush remaining
        if batch:
            session.execute(batch)
            inserted += len(batch)

        print("Successfully inserted data")

    except Exception as e:
        print("❌ Error during processing:", e)

# Run
session = create_session()
if session:
    process_and_insert_data(session)
else:
    print("❌ Session creation failed.")


# In[ ]:




