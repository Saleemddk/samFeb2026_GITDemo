"""
bank_details_GHCP.py
Generates 1000 rows of realistic Indian retail banking data and loads into MySQL.
Target: retail_banking.bank_details_GHCP
"""

import os
import random
import string
from datetime import date, datetime, timedelta
import mysql.connector

# ---------------------------------------------------------------------------
# Connection defaults (overridable via environment variables)
# ---------------------------------------------------------------------------
DB_HOST     = os.getenv("MYSQL_HOST",     "localhost")
DB_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
DB_USER     = os.getenv("MYSQL_USER",     "root")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "Admin123")
DB_SCHEMA   = os.getenv("MYSQL_SCHEMA",   "retail_banking")
TABLE_NAME  = "bank_details_GHCP"

# ---------------------------------------------------------------------------
# Realistic Indian data pools
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "Aarav", "Aditya", "Akash", "Amit", "Ananya", "Anjali", "Arjun", "Aryan",
    "Deepa", "Deepak", "Divya", "Gaurav", "Ishaan", "Ishita", "Kabir", "Kavya",
    "Kiran", "Kunal", "Lakshmi", "Manish", "Meera", "Mohit", "Neha", "Nikhil",
    "Nisha", "Pooja", "Priya", "Rahul", "Raj", "Rajan", "Rakesh", "Ravi",
    "Ritika", "Rohit", "Sachin", "Sanjay", "Sanjeev", "Sara", "Saurabh",
    "Shreya", "Shubham", "Simran", "Sneha", "Sunil", "Sunita", "Suresh",
    "Tanvi", "Uday", "Varun", "Vikas", "Vikram", "Vinay", "Vinita", "Vishal",
    "Yash", "Zara", "Pankaj", "Pallavi", "Nandini", "Mohan", "Madhuri",
    "Lalit", "Karishma", "Harsha", "Geeta", "Farhan", "Esha", "Chetan",
]

LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Singh", "Gupta", "Kumar", "Joshi", "Mehta",
    "Malhotra", "Chopra", "Agarwal", "Bose", "Chatterjee", "Das", "Desai",
    "Dutta", "Gandhi", "Iyer", "Jain", "Kapoor", "Khan", "Khanna", "Mishra",
    "Mukherjee", "Nair", "Pandey", "Pillai", "Rao", "Reddy", "Saxena",
    "Shah", "Shukla", "Sinha", "Srivastava", "Tiwari", "Trivedi", "Yadav",
    "Banerjee", "Bhat", "Chaudhary", "Dixit", "Dubey", "Ghosh", "Goyal",
    "Hegde", "Jha", "Kaur", "Kulkarni", "Menon", "Modi", "Naidu", "Patil",
    "Rajan", "Rathore", "Shetty", "Thakur", "Wagh", "Pawar", "Naik",
]

CITIES = [
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai",
    "Kolkata", "Surat", "Pune", "Jaipur", "Lucknow", "Kanpur", "Nagpur",
    "Indore", "Thane", "Bhopal", "Visakhapatnam", "Pimpri-Chinchwad",
    "Patna", "Vadodara", "Ghaziabad", "Ludhiana", "Agra", "Nashik",
    "Faridabad", "Meerut", "Rajkot", "Varanasi", "Srinagar", "Aurangabad",
    "Dhanbad", "Amritsar", "Navi Mumbai", "Allahabad", "Howrah", "Gwalior",
    "Jabalpur", "Coimbatore", "Vijayawada", "Jodhpur", "Madurai", "Raipur",
    "Kota", "Chandigarh", "Guwahati", "Solapur", "Hubli", "Tiruchirappalli",
    "Bareilly", "Mysore",
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.in", "rediffmail.com", "outlook.com"]

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def random_date(start_year: int = 2010) -> date:
    start = date(start_year, 1, 1)
    end   = date.today()
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_acc_number(existing: set) -> str:
    while True:
        num = "".join(random.choices(string.digits, k=12))
        if num not in existing:
            existing.add(num)
            return num


def random_phone() -> str:
    # Indian mobile: +91 followed by 6/7/8/9 then 9 more digits
    first_digit = random.choice(["6", "7", "8", "9"])
    rest = "".join(random.choices(string.digits, k=9))
    return f"+91{first_digit}{rest}"


def random_email(first: str, last: str, existing: set) -> str:
    base = f"{first.lower()}.{last.lower()}"
    domain = random.choice(EMAIL_DOMAINS)
    candidate = f"{base}@{domain}"
    if candidate not in existing:
        existing.add(candidate)
        return candidate
    # Append random suffix to guarantee uniqueness
    while True:
        suffix = random.randint(1, 9999)
        candidate = f"{base}{suffix}@{domain}"
        if candidate not in existing:
            existing.add(candidate)
            return candidate


def generate_rows(n: int = 1000) -> list:
    rows = []
    used_acc   = set()
    used_email = set()

    for _ in range(n):
        first     = random.choice(FIRST_NAMES)
        last      = random.choice(LAST_NAMES)
        nom_first = random.choice(FIRST_NAMES)
        nom_last  = random.choice(LAST_NAMES)
        nominee   = f"{nom_first} {nom_last}"

        acc_number        = random_acc_number(used_acc)
        city              = random.choice(CITIES)
        balance           = round(random.uniform(500, 2500000), 2)
        phone             = random_phone()
        email             = random_email(first, last, used_email)
        account_open_date = random_date(2010)
        record_created    = datetime.now()

        rows.append((
            first, last, acc_number, city, balance,
            nominee, phone, email, account_open_date, record_created
        ))

    return rows


# ---------------------------------------------------------------------------
# MySQL operations
# ---------------------------------------------------------------------------
DDL_CREATE_SCHEMA = f"CREATE DATABASE IF NOT EXISTS `{DB_SCHEMA}`"

DDL_DROP_TABLE = f"DROP TABLE IF EXISTS `{DB_SCHEMA}`.`{TABLE_NAME}`"

DDL_CREATE_TABLE = f"""
CREATE TABLE `{DB_SCHEMA}`.`{TABLE_NAME}` (
    id                  INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name                VARCHAR(100)   NOT NULL,
    last_name           VARCHAR(100)   NOT NULL,
    acc_number          VARCHAR(20)    NOT NULL UNIQUE,
    city                VARCHAR(100)   NOT NULL,
    balance             DECIMAL(15,2)  NOT NULL,
    nominee             VARCHAR(200)   NOT NULL,
    phone_number        VARCHAR(15)    NOT NULL,
    email               VARCHAR(255)   NOT NULL UNIQUE,
    account_open_date   DATE           NOT NULL,
    record_created_date DATETIME       NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

INSERT_SQL = f"""
INSERT INTO `{DB_SCHEMA}`.`{TABLE_NAME}`
    (name, last_name, acc_number, city, balance,
     nominee, phone_number, email, account_open_date, record_created_date)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def main():
    print("=" * 60)
    print("  Indian Retail Banking Data Loader — MySQL")
    print("=" * 60)

    # 1. Generate data
    print("\n[1/4] Generating 1000 rows of Indian banking data...")
    rows = generate_rows(1000)
    print(f"      Generated {len(rows)} rows.")

    # 2. Connect
    print(f"\n[2/4] Connecting to MySQL at {DB_HOST}:{DB_PORT} as {DB_USER}...")
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    cursor = conn.cursor()
    print("      Connected successfully.")

    # 3. Create schema, drop & recreate table
    print(f"\n[3/4] Setting up schema '{DB_SCHEMA}' and table '{TABLE_NAME}'...")
    cursor.execute(DDL_CREATE_SCHEMA)
    cursor.execute(DDL_DROP_TABLE)
    cursor.execute(DDL_CREATE_TABLE)
    conn.commit()
    print(f"      Table `{DB_SCHEMA}`.`{TABLE_NAME}` created.")

    # 4. Bulk insert
    print(f"\n[4/4] Inserting {len(rows)} rows using executemany...")
    cursor.executemany(INSERT_SQL, rows)
    conn.commit()
    print(f"      Insert complete. Rows committed: {cursor.rowcount}")

    # 5. Verify
    cursor.execute(f"SELECT COUNT(*) FROM `{DB_SCHEMA}`.`{TABLE_NAME}`")
    total = cursor.fetchone()[0]
    print(f"\n      Total rows in table : {total}")

    print("\n--- 10 Sample Rows ---")
    cursor.execute(
        f"SELECT id, name, last_name, acc_number, city, balance, "
        f"phone_number, email, account_open_date "
        f"FROM `{DB_SCHEMA}`.`{TABLE_NAME}` LIMIT 10"
    )
    col_names = [desc[0] for desc in cursor.description]
    print("  " + " | ".join(f"{c:<22}" for c in col_names))
    print("  " + "-" * (25 * len(col_names)))
    for row in cursor.fetchall():
        print("  " + " | ".join(f"{str(v):<22}" for v in row))

    cursor.close()
    conn.close()
    print("\nDone. Connection closed.")


if __name__ == "__main__":
    main()
