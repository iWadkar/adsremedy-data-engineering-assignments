import csv
import random
from faker import Faker
from datetime import date, timedelta

fake = Faker("en_IN")   # Indian locale
Faker.seed(42)

OUTPUT_FILE = "../data/employees_raw.csv"
NUM_RECORDS = 1200

job_titles = [
    "Software Engineer",
    "Senior Software Engineer",
    "Data Engineer",
    "Data Analyst",
    "Machine Learning Engineer",
    "Technical Lead",
    "Engineering Manager",
    "HR Executive"
]

departments = [
    "IT",
    "Data",
    "Analytics",
    "HR",
    "Finance",
    "Platform"
]

# Indian states (2-letter style, realistic for enterprise systems)
states = [
    "MH",  # Maharashtra
    "KA",  # Karnataka
    "TN",  # Tamil Nadu
    "DL",  # Delhi
    "TG",  # Telangana
    "AP",  # Andhra Pradesh
    "GJ",  # Gujarat
    "RJ",  # Rajasthan
    "UP",  # Uttar Pradesh
    "WB"   # West Bengal
]

def random_salary():
    base = random.randint(300000, 3000000)  # INR annual salary
    if random.random() < 0.4:
        return f"₹{base:,}"                 # ₹12,50,000
    return str(base)

def random_email(first, last):
    if random.random() < 0.15:
        return f"{first}@company"           # invalid email
    domain = random.choice(["company.com", "COMPANY.COM"])
    return f"{first}.{last}@{domain}"

rows = []

for i in range(NUM_RECORDS):
    # Inject duplicates ~10%
    employee_id = random.randint(1000, 1100) if random.random() < 0.1 else 5000 + i

    first = fake.first_name()
    last = fake.last_name()

    hire_date = fake.date_between(start_date="-10y", end_date="+2y")  # includes future
    birth_date = fake.date_of_birth(minimum_age=21, maximum_age=60)

    rows.append([
        employee_id,
        first.lower() if random.random() < 0.5 else first.upper(),
        last.lower() if random.random() < 0.5 else last.upper(),
        random_email(first, last),
        hire_date.isoformat(),
        random.choice(job_titles),
        random.choice(departments),
        random_salary(),
        random.choice([None, random.randint(5000, 5200)]),
        fake.address().replace("\n", " "),
        fake.city(),
        random.choice(states),
        fake.postcode(),
        birth_date.isoformat(),
        random.choice(["Active", "active", "INACTIVE", None])
    ])

with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "employee_id", "first_name", "last_name", "email", "hire_date",
        "job_title", "department", "salary", "manager_id", "address",
        "city", "state", "zip_code", "birth_date", "status"
    ])
    writer.writerows(rows)

print(f"Generated {len(rows)} records → {OUTPUT_FILE}")