"""
incremental_generator.py — Generates Day-2+ delta records for incremental load testing.

Simulates real-world changes:
1. New customers (5 new signups)
2. Changed customer addresses (SCD2 trigger — 10 address changes)
3. New accounts for existing customers
4. New transactions (200 new txns)
5. Account closures (3 accounts closed)
6. Loan defaults (2 loans defaulted)
7. Late-arriving transactions (old dates in new batch)
8. Duplicate source records (to test dedup handling)
9. Bad/invalid data (negative amounts, future dates, null required fields)
"""

import csv
import json
import os
import random
import string
from datetime import date, datetime, timedelta

from config.database_config import LANDING_ZONE, REFERENCE_ZONE


# Reuse same pools as main generator
FIRST_NAMES = [
    "Aarav", "Aditya", "Akash", "Amit", "Ananya", "Anjali", "Arjun", "Aryan",
    "Deepa", "Deepak", "Divya", "Gaurav", "Ishaan", "Ishita", "Kabir", "Kavya",
    "Kiran", "Kunal", "Lakshmi", "Manish", "Meera", "Mohit", "Neha", "Nikhil",
    "Nisha", "Pooja", "Priya", "Rahul", "Raj", "Rajan", "Shreya", "Varun",
]
LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Singh", "Gupta", "Kumar", "Joshi", "Mehta",
    "Malhotra", "Chopra", "Agarwal", "Bose", "Chatterjee", "Das", "Desai",
    "Rao", "Reddy", "Shah", "Sinha", "Yadav", "Banerjee", "Ghosh", "Goyal",
]
CITIES = [
    "Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Ahmedabad", "Chennai",
    "Kolkata", "Pune", "Jaipur", "Lucknow", "Nagpur", "Indore",
]
STATES = {
    "Mumbai": "Maharashtra", "Delhi": "Delhi", "Bengaluru": "Karnataka",
    "Hyderabad": "Telangana", "Ahmedabad": "Gujarat", "Chennai": "Tamil Nadu",
    "Kolkata": "West Bengal", "Pune": "Maharashtra", "Jaipur": "Rajasthan",
    "Lucknow": "Uttar Pradesh", "Nagpur": "Maharashtra", "Indore": "Madhya Pradesh",
}
EMAIL_DOMAINS = ["gmail.com", "yahoo.in", "rediffmail.com", "outlook.com"]
ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "SALARY"]
TXN_TYPES = ["CREDIT", "DEBIT"]
TXN_CHANNELS = ["NEFT", "RTGS", "IMPS", "UPI", "ATM", "BRANCH", "NET_BANKING", "MOBILE_BANKING"]
TXN_STATUS = ["SUCCESS", "FAILED", "PENDING"]


def _rand_phone():
    return f"+91{random.choice('6789')}{''.join(random.choices(string.digits, k=9))}"


def _rand_date(start_year=2024, end_date=None):
    start = date(start_year, 1, 1)
    end = end_date or date.today()
    if (end - start).days <= 0:
        return end
    return start + timedelta(days=random.randint(0, (end - start).days))


def _load_existing_customers():
    """Load existing customer IDs from landing zone."""
    path = os.path.join(LANDING_ZONE, "customers.csv")
    customers = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            customers.append(row)
    return customers


def _load_existing_accounts():
    """Load existing account IDs from landing zone."""
    path = os.path.join(LANDING_ZONE, "accounts.csv")
    accounts = []
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            accounts.append(row)
    return accounts


def generate_incremental():
    """Generate incremental (Day 2+) data with various scenarios."""
    os.makedirs(LANDING_ZONE, exist_ok=True)

    print("=" * 60)
    print("  Banking EDW — Incremental Data Generator (Day 2+)")
    print("=" * 60)

    existing_customers = _load_existing_customers()
    existing_accounts = _load_existing_accounts()

    # Track what we generate for summary
    summary = {}

    # =========================================================================
    # 1. NEW CUSTOMERS (5 brand new signups)
    # =========================================================================
    new_customers = []
    max_cust_id = max(int(c["customer_id"].replace("CUST", "")) for c in existing_customers)
    for i in range(1, 6):
        cid = f"CUST{max_cust_id + i:04d}"
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city = random.choice(CITIES)
        new_customers.append({
            "customer_id": cid,
            "first_name": first,
            "last_name": last,
            "date_of_birth": str(date(random.randint(1975, 2000), random.randint(1, 12), random.randint(1, 28))),
            "gender": random.choice(["M", "F"]),
            "phone": _rand_phone(),
            "email": f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(EMAIL_DOMAINS)}",
            "pan_number": f"{''.join(random.choices(string.ascii_uppercase, k=5))}{''.join(random.choices(string.digits, k=4))}{''.join(random.choices(string.ascii_uppercase, k=1))}",
            "aadhar_number": "".join(random.choices(string.digits, k=12)),
            "address": f"{random.randint(1, 500)} {random.choice(['MG Road', 'Park Street', 'Nehru Nagar', 'Sector 21', 'Lake View'])}",
            "city": city,
            "state": STATES.get(city, "Maharashtra"),
            "pin_code": str(random.randint(100000, 999999)),
            "customer_since": str(date.today()),
            "is_active": "Y",
        })
    summary["new_customers"] = len(new_customers)

    # =========================================================================
    # 2. CHANGED CUSTOMERS — address changes (SCD2 triggers) — 10 records
    # =========================================================================
    changed_customers = []
    sample_for_change = random.sample(existing_customers, min(10, len(existing_customers)))
    for cust in sample_for_change:
        modified = dict(cust)
        new_city = random.choice([c for c in CITIES if c != cust.get("city", "")])
        modified["city"] = new_city
        modified["state"] = STATES.get(new_city, "Maharashtra")
        modified["address"] = f"{random.randint(1, 999)} {random.choice(['New Lane', 'Cross Road', 'Ring Road', 'Main Street'])}"
        modified["pin_code"] = str(random.randint(100000, 999999))
        changed_customers.append(modified)
    summary["changed_customers_address"] = len(changed_customers)

    # =========================================================================
    # 3. DUPLICATE RECORDS (to test dedup) — repeat 3 existing customers
    # =========================================================================
    duplicate_customers = random.sample(existing_customers, min(3, len(existing_customers)))
    summary["duplicate_customers"] = len(duplicate_customers)

    # Write combined customers file (new + changed + duplicates)
    all_incr_customers = new_customers + changed_customers + duplicate_customers
    cust_path = os.path.join(LANDING_ZONE, "customers.csv")
    fieldnames = list(existing_customers[0].keys())
    with open(cust_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_incr_customers)
    print(f"  Written {len(all_incr_customers)} rows → customers.csv (incremental)")

    # =========================================================================
    # 4. NEW ACCOUNTS for new customers
    # =========================================================================
    max_acc_id = max(int(a["account_id"].replace("ACC", "")) for a in existing_accounts)
    new_accounts = []
    for i, cust in enumerate(new_customers, 1):
        acc_id = f"ACC{max_acc_id + i:05d}"
        branch_id = f"BR{random.randint(1, 50):03d}"
        new_accounts.append({
            "account_id": acc_id,
            "customer_id": cust["customer_id"],
            "account_number": "".join(random.choices(string.digits, k=12)),
            "account_type": random.choice(ACCOUNT_TYPES),
            "product_id": f"PRD{random.randint(1, 5):03d}",
            "branch_id": branch_id,
            "ifsc_code": f"BNKE0{branch_id.replace('BR', '')}",
            "balance": str(round(random.uniform(1000, 500000), 2)),
            "currency": "INR",
            "open_date": str(date.today()),
            "close_date": "",
            "status": "ACTIVE",
            "nominee_name": f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        })

    # =========================================================================
    # 5. ACCOUNT CLOSURES — mark 3 existing accounts as CLOSED
    # =========================================================================
    active_accounts = [a for a in existing_accounts if a.get("status") == "ACTIVE"]
    closed_accounts = []
    for acc in random.sample(active_accounts, min(3, len(active_accounts))):
        modified = dict(acc)
        modified["status"] = "CLOSED"
        modified["close_date"] = str(date.today())
        closed_accounts.append(modified)
    summary["account_closures"] = len(closed_accounts)

    # Write accounts (new + closed)
    all_incr_accounts = new_accounts + closed_accounts
    acc_path = os.path.join(LANDING_ZONE, "accounts.csv")
    acc_fieldnames = list(existing_accounts[0].keys())
    with open(acc_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=acc_fieldnames)
        writer.writeheader()
        writer.writerows(all_incr_accounts)
    print(f"  Written {len(all_incr_accounts)} rows → accounts.csv (incremental)")
    summary["new_accounts"] = len(new_accounts)

    # =========================================================================
    # 6. NEW TRANSACTIONS (200 normal + 5 late-arriving + 5 bad records)
    # =========================================================================
    all_account_ids = [a["account_id"] for a in existing_accounts] + [a["account_id"] for a in new_accounts]
    max_txn = 5000  # from initial load

    transactions = []
    # 200 normal new transactions (recent dates)
    for i in range(1, 201):
        txn_id = f"TXN{max_txn + i:07d}"
        txn_date = _rand_date(2026)
        transactions.append({
            "transaction_id": txn_id,
            "account_id": random.choice(all_account_ids),
            "transaction_date": str(txn_date),
            "transaction_time": f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "transaction_type": random.choice(TXN_TYPES),
            "amount": str(round(random.uniform(100, 100000), 2)),
            "currency": "INR",
            "channel": random.choice(TXN_CHANNELS),
            "status": random.choice(TXN_STATUS),
            "description": random.choice(["Salary", "Rent", "EMI", "Shopping", "Transfer", "Bill Payment"]),
            "beneficiary_acc": "".join(random.choices(string.digits, k=12)),
            "reference_number": f"REF{''.join(random.choices(string.digits, k=10))}",
        })

    # 5 LATE-ARRIVING transactions (dates from 2023 — old data arriving late)
    for i in range(201, 206):
        txn_id = f"TXN{max_txn + i:07d}"
        txn_date = date(2023, random.randint(1, 12), random.randint(1, 28))
        transactions.append({
            "transaction_id": txn_id,
            "account_id": random.choice(all_account_ids[:50]),  # use existing accounts
            "transaction_date": str(txn_date),
            "transaction_time": f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
            "transaction_type": random.choice(TXN_TYPES),
            "amount": str(round(random.uniform(500, 50000), 2)),
            "currency": "INR",
            "channel": "BRANCH",
            "status": "SUCCESS",
            "description": "Late-arriving reconciliation",
            "beneficiary_acc": "".join(random.choices(string.digits, k=12)),
            "reference_number": f"LATE{''.join(random.choices(string.digits, k=8))}",
        })

    # 5 BAD RECORDS (negative amount, future date, empty account_id)
    bad_records = [
        # Negative amount
        {"transaction_id": f"TXN{max_txn + 206:07d}", "account_id": random.choice(all_account_ids),
         "transaction_date": str(date.today()), "transaction_time": "10:00:00",
         "transaction_type": "DEBIT", "amount": "-5000.00", "currency": "INR",
         "channel": "UPI", "status": "SUCCESS", "description": "Negative amount test",
         "beneficiary_acc": "000000000000", "reference_number": "BADREF001"},
        # Future date
        {"transaction_id": f"TXN{max_txn + 207:07d}", "account_id": random.choice(all_account_ids),
         "transaction_date": "2030-12-31", "transaction_time": "12:00:00",
         "transaction_type": "CREDIT", "amount": "10000.00", "currency": "INR",
         "channel": "NEFT", "status": "SUCCESS", "description": "Future date test",
         "beneficiary_acc": "000000000000", "reference_number": "BADREF002"},
        # Empty account_id
        {"transaction_id": f"TXN{max_txn + 208:07d}", "account_id": "",
         "transaction_date": str(date.today()), "transaction_time": "08:00:00",
         "transaction_type": "CREDIT", "amount": "25000.00", "currency": "INR",
         "channel": "BRANCH", "status": "SUCCESS", "description": "Missing account test",
         "beneficiary_acc": "000000000000", "reference_number": "BADREF003"},
        # Invalid date format
        {"transaction_id": f"TXN{max_txn + 209:07d}", "account_id": random.choice(all_account_ids),
         "transaction_date": "31-12-2025", "transaction_time": "14:00:00",
         "transaction_type": "DEBIT", "amount": "7500.00", "currency": "INR",
         "channel": "ATM", "status": "SUCCESS", "description": "Bad date format test",
         "beneficiary_acc": "000000000000", "reference_number": "BADREF004"},
        # Zero amount
        {"transaction_id": f"TXN{max_txn + 210:07d}", "account_id": random.choice(all_account_ids),
         "transaction_date": str(date.today()), "transaction_time": "16:00:00",
         "transaction_type": "DEBIT", "amount": "0.00", "currency": "INR",
         "channel": "UPI", "status": "SUCCESS", "description": "Zero amount test",
         "beneficiary_acc": "000000000000", "reference_number": "BADREF005"},
    ]
    transactions.extend(bad_records)

    # Write transactions
    txn_path = os.path.join(LANDING_ZONE, "transactions.csv")
    txn_fields = list(transactions[0].keys())
    with open(txn_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=txn_fields)
        writer.writeheader()
        writer.writerows(transactions)
    print(f"  Written {len(transactions)} rows → transactions.csv (incremental)")
    summary["new_transactions"] = 200
    summary["late_arriving_transactions"] = 5
    summary["bad_records"] = 5

    # =========================================================================
    # 7. LOAN DEFAULTS — update 2 existing active loans to DEFAULT
    # =========================================================================
    loans_path = os.path.join(LANDING_ZONE, "loans.csv")
    existing_loans = []
    with open(loans_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        loan_fieldnames = reader.fieldnames
        for row in reader:
            existing_loans.append(row)

    active_loans = [l for l in existing_loans if l.get("status") == "ACTIVE"]
    defaulted_loans = []
    for loan in random.sample(active_loans, min(2, len(active_loans))):
        modified = dict(loan)
        modified["status"] = "DEFAULT"
        defaulted_loans.append(modified)
    summary["loan_defaults"] = len(defaulted_loans)

    # Write loans (only the changed ones for incremental)
    with open(loans_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=loan_fieldnames)
        writer.writeheader()
        writer.writerows(defaulted_loans)
    print(f"  Written {len(defaulted_loans)} rows → loans.csv (incremental - defaults)")

    # =========================================================================
    # 8. BRANCHES — no change (keep existing file)
    # =========================================================================
    # Don't overwrite branches.json — no changes for incremental

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "-" * 60)
    print("  Incremental Data Summary:")
    print("-" * 60)
    for key, val in summary.items():
        print(f"    {key:35s}: {val}")
    print("-" * 60)
    print("  Scenarios included:")
    print("    - New customer signups (SCD2: new rows)")
    print("    - Customer address changes (SCD2: version existing)")
    print("    - Duplicate source records (dedup testing)")
    print("    - Account closures (status change)")
    print("    - New transactions (normal append)")
    print("    - Late-arriving transactions (old dates, new batch)")
    print("    - Bad data (negative amounts, future dates, nulls)")
    print("    - Loan defaults (status update)")
    print("\n  Incremental generation complete.")


if __name__ == "__main__":
    generate_incremental()
