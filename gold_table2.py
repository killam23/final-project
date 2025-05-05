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


# In[ ]:


### GOLD TABLE 2 (ML MODEL)

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import numpy as np

# Step 1: Connect to Cassandra cluster and create session
def create_session():
    cloud_config = {'secure_connect_bundle': 'secure-connect-finalproject.zip'}
    auth_provider = PlainTextAuthProvider('OHcWaOTFhHqZggUSlhIZUgIt', 'unLZx3Nk+BL0naBfSn_QvSUFHwmEddiF3NiG.EGSwdrwnvc65+,2-MS,,xhirlTDXXkfKAwKUzyAM9JUsNn5T6sAzbertMXMBDe.A.35RUErsaZwTeroJPdBvzXb,XMT')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace('project')  # Set your keyspace here
    return session

session = create_session()

# Step 2: Retrieve data from the Silver table
rows = session.execute("SELECT founded, industry, number_of_employees FROM silver_organizations")
data = []
for row in rows:
    if row.founded and row.industry and row.number_of_employees:
        data.append([row.founded, row.industry, row.number_of_employees])

# Step 3: Preprocess the data (One-Hot Encoding for 'industry')
industries = list(set([row[1] for row in data]))  # Extract unique industries
industry_index = {industry: i for i, industry in enumerate(industries)}  # Map each industry to an index

X = []
y = []

# One-Hot Encoding for 'industry' and storing the data in X, y
for row in data:
    founded = row[0]
    industry = row[1]
    number_of_employees = row[2]
    
    industry_encoded = [0] * len(industries)
    industry_encoded[industry_index[industry]] = 1
    
    X.append([founded] + industry_encoded)
    y.append(number_of_employees)

# Step 4: Train a Linear Regression model
X = np.array(X)
y = np.array(y)

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = LinearRegression()
model.fit(X_train, y_train)

# Step 5: Evaluate the model
preds = model.predict(X_test)
rmse = mean_squared_error(y_test, preds, squared=False)
print(f"📉 ML Model RMSE: {rmse:.2f}")

# Step 6: Insert model predictions into Gold table
insert_gold_query = """
INSERT INTO gold_predictions (founded, industry, predicted_number_of_employees)
VALUES (%s, %s, %s)
"""

# Loop through the test data and insert predictions into Gold table
for i in range(len(X_test)):
    founded_value = X_test[i][0]
    industry_value = industries[np.argmax(X_test[i][1:])]  # Get industry from one-hot encoding
    predicted_value = preds[i]
    
    # Insert the data into the Gold table
    session.execute(insert_gold_query, (founded_value, industry_value, int(predicted_value)))

print("Predictions successfully inserted into the Gold table.")


# In[ ]:




