import csv
import random
from datetime import datetime, timedelta

OUTPUT_PATH = "data/raw/ecommerce_raw.csv"
NUM_ROWS = 120_000

products = [
    ("Laptop", "Electronics"),
    ("Smartphone", "Electronics"),
    ("Headphones", "Electronics"),
    ("T-Shirt", "Fashion"),
    ("Jeans", "Fashion"),
    ("Sneakers", "Fashion"),
    ("Blender", "Home"),
    ("Coffee Maker", "Home"),
    ("Chair", "Home"),
]

start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 12, 31)

def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))

with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "transaction_id",
        "customer_id",
        "product",
        "category",
        "price",
        "quantity",
        "transaction_date"
    ])

    for i in range(1, NUM_ROWS + 1):
        product, category = random.choice(products)
        price = round(random.uniform(10, 1500), 2)
        quantity = random.randint(1, 5)
        date = random_date(start_date, end_date).strftime("%Y-%m-%d")

        writer.writerow([
            f"T{i:07d}",
            f"C{random.randint(1, 20000):05d}",
            product,
            category,
            price,
            quantity,
            date
        ])

print(f"Dataset generated: {OUTPUT_PATH} ({NUM_ROWS} rows)")

