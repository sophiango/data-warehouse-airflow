from datetime import datetime, timedelta
import os, random
import pandas as pd
import uuid

from faker import Faker


ROOT = os.path.dirname(os.path.dirname(__file__))
RAW_DIR = os.path.join(ROOT, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

fake = Faker()
Faker.seed(42)
random.seed(42)

N_CUSTOMERS = 500
N_ORDERS = 2000

regions = ["NA", "EMEA", "APAC", "LATAM"]
# use age group instead of age for customer privacy (k-anonymity)
age_groups = ["18-24", "25-34", "35-44", "45-54", "55+"]
categories = ["electronics", "home", "toys", "books", "beauty", "sports"]

# customers
customers = []
for _ in range(N_CUSTOMERS):
    customers.append({
        "customer_id": str(uuid.uuid4()),
        "signup_date": fake.date_between(start_date="-360d", end_date="today"),
        "region": random.choice(regions),
        "age_group": random.choice(age_groups)
    })

customer_df = pd.DataFrame(customers)

# orders
start = datetime.now() - timedelta(days=120)
orders = []
for _ in range(N_ORDERS):
    random_ts = start + timedelta(minutes=random.randint(0, 120*24*60))
    random_status = random.choices(["completed", "refunded", "cancelled"], weights=[0.9, 0.05, 0.05])[0]
    random_price = round(random.uniform(5, 400), 2),
    orders.append({
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(customers)["customer_id"],
        "order_ts": random_ts.isoformat(timespec="seconds"),
        "product_category": random.choice(categories),
        "price": random_price,
        "quantity": random.randint(1, 5),
        "status": random_status
    })

order_df = pd.DataFrame(orders)

customer_file_path = os.path.join(RAW_DIR, "customers.csv")
order_file_path = os.path.join(RAW_DIR, "orders.csv")
customer_df.to_csv(customer_file_path, index=False)
order_df.to_csv(order_file_path, index=False)
print("wrote", customer_file_path, order_file_path)
