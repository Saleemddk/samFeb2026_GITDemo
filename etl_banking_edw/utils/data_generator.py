"""
Data generator — produces realistic Indian banking data for all source tables.
Generates CSV/JSON files in the landing zone to simulate S3 ingestion.
"""

import csv
import json
import os
import random
import string
from datetime import date, datetime, timedelta

from config.database_config import LANDING_ZONE, REFERENCE_ZONE

# ---------------------------------------------------------------------------
# Indian data pools
# ---------------------------------------------------------------------------
FIRST_NAMES = [
    "Aarav", "Aditya", "Akash", "Amit", "Ananya", "Anjali", "Arjun", "Aryan",
    "Deepa", "Deepak", "Divya", "Gaurav", "Ishaan", "Ishita", "Kabir", "Kavya",
    "Kiran", "Kunal", "Lakshmi", "Manish", "Meera", "Mohit", "Neha", "Nikhil",
    "Nisha", "Pooja", "Priya", "Rahul", "Raj", "Rajan", "Rakesh", "Ravi",
    "Ritika", "Rohit", "Sachin", "Sanjay", "Sara", "Saurabh", "Shreya",
    "Shubham", "Simran", "Sneha", "Sunil", "Sunita", "Suresh", "Tanvi",
    "Uday", "Varun", "Vikas", "Vikram", "Vinay", "Vishal", "Yash", "Zara",
    "Pankaj", "Pallavi", "Nandini", "Mohan", "Madhuri", "Harsha", "Geeta",
    "Farhan", "Esha", "Chetan", "Bhavya", "Arun", "Anand", "Swati", "Rekha",
]

LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Singh", "Gupta", "Kumar", "Joshi", "Mehta",
    "Malhotra", "Chopra", "Agarwal", "Bose", "Chatterjee", "Das", "Desai",
    "Dutta", "Gandhi", "Iyer", "Jain", "Kapoor", "Khan", "Khanna", "Mishra",
    "Mukherjee", "Nair", "Pandey", "Pillai", "Rao", "Reddy", "Saxena",
    "Shah", "Shukla", "Sinha", "Srivastava", "Tiwari", "Trivedi", "Yadav",
    "Banerjee", "Bhat", "Chaudhary", "Dixit", "Dubey", "Ghosh", "Goyal",
    "Hegde", "Jha", "Kaur", "Kulkarni", "Menon", "Patil", "Thakur", "Naik",
]

CITIES = [
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai",
    "Kolkata", "Surat", "Pune", "Jaipur", "Lucknow", "Kanpur", "Nagpur",
    "Indore", "Thane", "Bhopal", "Visakhapatnam", "Patna", "Vadodara",
    "Ghaziabad", "Ludhiana", "Agra", "Nashik", "Faridabad", "Meerut",
    "Rajkot", "Varanasi", "Srinagar", "Aurangabad", "Amritsar", "Chandigarh",
    "Coimbatore", "Madurai", "Raipur", "Jodhpur", "Guwahati", "Mysore",
]

STATES = {
    "Mumbai": "Maharashtra", "Delhi": "Delhi", "Bengaluru": "Karnataka",
    "Hyderabad": "Telangana", "Ahmedabad": "Gujarat", "Chennai": "Tamil Nadu",
    "Kolkata": "West Bengal", "Surat": "Gujarat", "Pune": "Maharashtra",
    "Jaipur": "Rajasthan", "Lucknow": "Uttar Pradesh", "Kanpur": "Uttar Pradesh",
    "Nagpur": "Maharashtra", "Indore": "Madhya Pradesh", "Thane": "Maharashtra",
    "Bhopal": "Madhya Pradesh", "Visakhapatnam": "Andhra Pradesh",
    "Patna": "Bihar", "Vadodara": "Gujarat", "Ghaziabad": "Uttar Pradesh",
    "Ludhiana": "Punjab", "Agra": "Uttar Pradesh", "Nashik": "Maharashtra",
    "Faridabad": "Haryana", "Meerut": "Uttar Pradesh", "Rajkot": "Gujarat",
    "Varanasi": "Uttar Pradesh", "Srinagar": "Jammu & Kashmir",
    "Aurangabad": "Maharashtra", "Amritsar": "Punjab", "Chandigarh": "Chandigarh",
    "Coimbatore": "Tamil Nadu", "Madurai": "Tamil Nadu", "Raipur": "Chhattisgarh",
    "Jodhpur": "Rajasthan", "Guwahati": "Assam", "Mysore": "Karnataka",
}

EMAIL_DOMAINS = ["gmail.com", "yahoo.in", "rediffmail.com", "outlook.com"]
ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "SALARY", "NRI", "FIXED_DEPOSIT", "RECURRING_DEPOSIT"]
LOAN_TYPES = ["HOME", "PERSONAL", "CAR", "EDUCATION", "GOLD", "BUSINESS"]
LOAN_STATUS = ["ACTIVE", "CLOSED", "DEFAULT", "RESTRUCTURED"]
TXN_TYPES = ["CREDIT", "DEBIT"]
TXN_CHANNELS = ["NEFT", "RTGS", "IMPS", "UPI", "ATM", "BRANCH", "NET_BANKING", "MOBILE_BANKING"]
TXN_STATUS = ["SUCCESS", "FAILED", "PENDING", "REVERSED"]

PRODUCT_TYPES = [
    {"product_id": "PRD001", "product_name": "Basic Savings",      "product_category": "SAVINGS",    "interest_rate": 3.5},
    {"product_id": "PRD002", "product_name": "Premium Savings",    "product_category": "SAVINGS",    "interest_rate": 4.0},
    {"product_id": "PRD003", "product_name": "Current Account",    "product_category": "CURRENT",    "interest_rate": 0.0},
    {"product_id": "PRD004", "product_name": "Salary Account",     "product_category": "SALARY",     "interest_rate": 3.5},
    {"product_id": "PRD005", "product_name": "NRI Savings",        "product_category": "NRI",        "interest_rate": 5.0},
    {"product_id": "PRD006", "product_name": "Fixed Deposit 1Y",   "product_category": "FD",         "interest_rate": 6.5},
    {"product_id": "PRD007", "product_name": "Fixed Deposit 3Y",   "product_category": "FD",         "interest_rate": 7.0},
    {"product_id": "PRD008", "product_name": "Recurring Deposit",  "product_category": "RD",         "interest_rate": 6.0},
    {"product_id": "PRD009", "product_name": "Home Loan",          "product_category": "LOAN",       "interest_rate": 8.5},
    {"product_id": "PRD010", "product_name": "Personal Loan",      "product_category": "LOAN",       "interest_rate": 12.0},
    {"product_id": "PRD011", "product_name": "Car Loan",           "product_category": "LOAN",       "interest_rate": 9.5},
    {"product_id": "PRD012", "product_name": "Education Loan",     "product_category": "LOAN",       "interest_rate": 8.0},
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _rand_date(start_year=2010, end_date=None):
    start = date(start_year, 1, 1)
    end = end_date or date.today()
    return start + timedelta(days=random.randint(0, (end - start).days))


def _rand_phone():
    return f"+91{random.choice('6789')}{''.join(random.choices(string.digits, k=9))}"


def _rand_email(first, last, used):
    base = f"{first.lower()}.{last.lower()}"
    domain = random.choice(EMAIL_DOMAINS)
    email = f"{base}@{domain}"
    if email not in used:
        used.add(email)
        return email
    while True:
        email = f"{base}{random.randint(1, 9999)}@{domain}"
        if email not in used:
            used.add(email)
            return email


def _rand_acc(used):
    while True:
        num = "".join(random.choices(string.digits, k=12))
        if num not in used:
            used.add(num)
            return num


def _rand_ifsc():
    bank_codes = ["SBIN", "HDFC", "ICIC", "AXIS", "PUNB", "BARB", "CNRB", "UBIN", "IDIB", "BKID"]
    return f"{random.choice(bank_codes)}0{random.randint(100000, 999999)}"


# ---------------------------------------------------------------------------
# Generators
# ---------------------------------------------------------------------------
def generate_customers(n=500):
    """Generate n customer records."""
    rows = []
    used_emails = set()
    for i in range(1, n + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city = random.choice(CITIES)
        rows.append({
            "customer_id":    f"CUST{i:06d}",
            "first_name":     first,
            "last_name":      last,
            "date_of_birth":  str(_rand_date(1960, date(2003, 1, 1))),
            "gender":         random.choice(["M", "F"]),
            "phone":          _rand_phone(),
            "email":          _rand_email(first, last, used_emails),
            "pan_number":     "".join(random.choices(string.ascii_uppercase, k=5)) + "".join(random.choices(string.digits, k=4)) + random.choice(string.ascii_uppercase),
            "aadhar_number":  "".join(random.choices(string.digits, k=12)),
            "address":        f"{random.randint(1, 999)}, {random.choice(['MG Road', 'Station Road', 'Ring Road', 'Lake Road', 'Hill Road', 'Park Street', 'Gandhi Nagar', 'Nehru Place'])}",
            "city":           city,
            "state":          STATES.get(city, "Maharashtra"),
            "pin_code":       str(random.randint(100000, 999999)),
            "customer_since": str(_rand_date(2008)),
            "is_active":      random.choice(["Y", "Y", "Y", "Y", "N"]),  # 80% active
        })
    return rows


def generate_accounts(customers, avg_per_customer=2):
    """Generate account records for customers."""
    rows = []
    used_acc = set()
    acc_id = 1
    for cust in customers:
        num_accounts = random.choices([1, 2, 3], weights=[40, 45, 15])[0]
        for _ in range(num_accounts):
            open_date = _rand_date(2010)
            product = random.choice(PRODUCT_TYPES[:8])  # non-loan products
            close_dt = ""
            if random.random() < 0.05:
                close_start = date(open_date.year + 1, 1, 1)
                if close_start < date.today():
                    close_dt = str(_rand_date(open_date.year + 1))
            rows.append({
                "account_id":      f"ACC{acc_id:08d}",
                "customer_id":     cust["customer_id"],
                "account_number":  _rand_acc(used_acc),
                "account_type":    product["product_category"],
                "product_id":      product["product_id"],
                "branch_id":       f"BR{random.randint(1, 50):04d}",
                "ifsc_code":       _rand_ifsc(),
                "balance":         round(random.uniform(500, 2500000), 2),
                "currency":        "INR",
                "open_date":       str(open_date),
                "close_date":      close_dt,
                "status":          random.choice(["ACTIVE", "ACTIVE", "ACTIVE", "DORMANT", "CLOSED"]),
                "nominee_name":    f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            })
            acc_id += 1
    return rows


def generate_transactions(accounts, n=5000):
    """Generate n transaction records."""
    active_accounts = [a for a in accounts if a["status"] != "CLOSED"]
    rows = []
    for i in range(1, n + 1):
        acc = random.choice(active_accounts)
        txn_type = random.choice(TXN_TYPES)
        amount = round(random.uniform(10, 500000), 2) if txn_type == "CREDIT" else round(random.uniform(10, 200000), 2)
        txn_date = _rand_date(2023)
        rows.append({
            "transaction_id":   f"TXN{i:010d}",
            "account_id":       acc["account_id"],
            "transaction_date": str(txn_date),
            "transaction_time": f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "transaction_type": txn_type,
            "amount":           amount,
            "currency":         "INR",
            "channel":          random.choice(TXN_CHANNELS),
            "status":           random.choices(TXN_STATUS, weights=[85, 5, 5, 5])[0],
            "description":      random.choice([
                "Salary Credit", "ATM Withdrawal", "UPI Transfer", "NEFT Transfer",
                "Bill Payment", "EMI Debit", "Online Shopping", "Fund Transfer",
                "Interest Credit", "Cash Deposit", "Rent Payment", "Insurance Premium",
                "Mutual Fund SIP", "FD Maturity Credit", "Loan Disbursement",
            ]),
            "beneficiary_acc":  "".join(random.choices(string.digits, k=12)) if txn_type == "DEBIT" else "",
            "reference_number": "".join(random.choices(string.ascii_uppercase + string.digits, k=16)),
        })
    return rows


def generate_loans(customers, n=300):
    """Generate loan records."""
    rows = []
    for i in range(1, n + 1):
        cust = random.choice(customers)
        loan_type = random.choice(LOAN_TYPES)
        disburse_date = _rand_date(2015)
        tenure_months = random.choice([12, 24, 36, 60, 84, 120, 180, 240])
        principal = round(random.uniform(50000, 10000000), 2)
        rate = round(random.uniform(7.5, 15.0), 2)
        rows.append({
            "loan_id":           f"LN{i:08d}",
            "customer_id":       cust["customer_id"],
            "loan_type":         loan_type,
            "product_id":        random.choice([p["product_id"] for p in PRODUCT_TYPES if p["product_category"] == "LOAN"]),
            "principal_amount":  principal,
            "interest_rate":     rate,
            "tenure_months":     tenure_months,
            "emi_amount":        round(principal * rate / (100 * 12) * (1 + rate / (100 * 12)) ** tenure_months / ((1 + rate / (100 * 12)) ** tenure_months - 1), 2),
            "disburse_date":     str(disburse_date),
            "maturity_date":     str(disburse_date + timedelta(days=tenure_months * 30)),
            "outstanding_amount": round(random.uniform(0, principal), 2),
            "status":            random.choice(LOAN_STATUS),
            "branch_id":         f"BR{random.randint(1, 50):04d}",
            "collateral_type":   random.choice(["PROPERTY", "GOLD", "FD", "NONE", "VEHICLE"]) if loan_type != "PERSONAL" else "NONE",
        })
    return rows


def generate_branches(n=50):
    """Generate branch records."""
    rows = []
    for i in range(1, n + 1):
        city = CITIES[i % len(CITIES)]
        rows.append({
            "branch_id":      f"BR{i:04d}",
            "branch_name":    f"{city} {'Main' if i % 3 == 0 else random.choice(['Central', 'East', 'West', 'North', 'South'])} Branch",
            "branch_code":    f"BR{i:04d}",
            "ifsc_code":      _rand_ifsc(),
            "address":        f"{random.randint(1, 500)}, {random.choice(['MG Road', 'Station Road', 'Ring Road'])}",
            "city":           city,
            "state":          STATES.get(city, "Maharashtra"),
            "pin_code":       str(random.randint(100000, 999999)),
            "phone":          _rand_phone(),
            "manager_name":   f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
            "established_date": str(_rand_date(1990, date(2020, 1, 1))),
            "is_active":      "Y",
        })
    return rows


# ---------------------------------------------------------------------------
# Write to files
# ---------------------------------------------------------------------------
def write_csv(rows, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written {len(rows)} rows → {filepath}")


def write_json(rows, filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=str)
    print(f"  Written {len(rows)} rows → {filepath}")


def generate_all():
    """Generate all source data files."""
    print("=" * 60)
    print("  Banking EDW — Source Data Generator")
    print("=" * 60)

    # Core entities
    customers = generate_customers(500)
    accounts  = generate_accounts(customers)
    txns      = generate_transactions(accounts, 5000)
    loans     = generate_loans(customers, 300)
    branches  = generate_branches(50)

    # Landing zone files (simulating S3 drops)
    write_csv(customers, os.path.join(LANDING_ZONE, "customers.csv"))
    write_csv(accounts,  os.path.join(LANDING_ZONE, "accounts.csv"))
    write_csv(txns,      os.path.join(LANDING_ZONE, "transactions.csv"))
    write_csv(loans,     os.path.join(LANDING_ZONE, "loans.csv"))
    write_json(branches, os.path.join(LANDING_ZONE, "branches.json"))

    # Reference data
    write_csv(PRODUCT_TYPES, os.path.join(REFERENCE_ZONE, "product_types.csv"))

    print(f"\n  Customers : {len(customers)}")
    print(f"  Accounts  : {len(accounts)}")
    print(f"  Transactions: {len(txns)}")
    print(f"  Loans     : {len(loans)}")
    print(f"  Branches  : {len(branches)}")
    print(f"  Products  : {len(PRODUCT_TYPES)}")
    print("\nData generation complete.")
    return customers, accounts, txns, loans, branches


if __name__ == "__main__":
    generate_all()
