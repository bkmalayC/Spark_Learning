# =========================
# 🧠 Create Nested Parquet File in Google Colab
# =========================

# Step 1 — Install PyArrow
!pip install pyarrow --quiet

# Step 2 — Import libraries
import pyarrow as pa
import pyarrow.parquet as pq

# Step 3 — Define nested schema
schema = pa.schema([
    ("customer_id", pa.string()),
    ("name", pa.struct([
        ("first", pa.string()),
        ("last", pa.string())
    ])),
    ("accounts", pa.list_(pa.struct([
        ("account_type", pa.string()),
        ("balance", pa.int32()),
        ("branch", pa.string())
    ]))),
    ("transactions", pa.list_(pa.struct([
        ("id", pa.string()),
        ("date", pa.string()),
        ("type", pa.string()),
        ("amount", pa.int32())
    ])))
])

# Step 4 — Create nested data
data = [
    {
        "customer_id": "C001",
        "name": {"first": "Amit", "last": "Sharma"},
        "accounts": [
            {"account_type": "Savings", "balance": 18000, "branch": "Mumbai"},
            {"account_type": "Credit", "balance": -2000, "branch": "Mumbai"}
        ],
        "transactions": [
            {"id": "T001", "date": "2025-10-28", "type": "DEPOSIT", "amount": 15000},
            {"id": "T002", "date": "2025-10-30", "type": "WITHDRAWAL", "amount": 2000},
            {"id": "T007", "date": "2025-11-03", "type": "DEPOSIT", "amount": 5000}
        ]
    },
    {
        "customer_id": "C002",
        "name": {"first": "Priya", "last": "Verma"},
        "accounts": [
            {"account_type": "Savings", "balance": 15000, "branch": "Delhi"}
        ],
        "transactions": [
            {"id": "T003", "date": "2025-10-29", "type": "DEPOSIT", "amount": 30000},
            {"id": "T004", "date": "2025-10-31", "type": "TRANSFER", "amount": 5000},
            {"id": "T008", "date": "2025-11-04", "type": "WITHDRAWAL", "amount": 10000}
        ]
    }
]

# Step 5 — Write to Parquet
table = pa.Table.from_pylist(data, schema=schema)
pq.write_table(table, "nested_transactions.parquet")

print("✅ nested_transactions.parquet created successfully!")

# Step 6 — Download the file to your computer
from google.colab import files
files.download("nested_transactions.parquet")