import sqlite3
import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

DB_PATH = "india_ops.db"

def get_connection():
    return sqlite3.connect(DB_PATH)

# ─────────────────────────────────────────────────────────────────────────────
#  REAL INDIA MASTER DATA
# ─────────────────────────────────────────────────────────────────────────────
STATES_CITIES = {
    "Maharashtra":      ["Mumbai", "Pune", "Nagpur", "Nashik", "Aurangabad", "Thane"],
    "Karnataka":        ["Bengaluru", "Mysuru", "Mangaluru", "Hubli", "Belagavi"],
    "Tamil Nadu":       ["Chennai", "Coimbatore", "Madurai", "Salem", "Tiruchirappalli"],
    "Delhi":            ["New Delhi", "Noida", "Gurgaon", "Faridabad", "Dwarka"],
    "Uttar Pradesh":    ["Lucknow", "Kanpur", "Agra", "Varanasi", "Prayagraj", "Meerut"],
    "West Bengal":      ["Kolkata", "Howrah", "Siliguri", "Asansol", "Durgapur"],
    "Gujarat":          ["Ahmedabad", "Surat", "Vadodara", "Rajkot", "Gandhinagar"],
    "Rajasthan":        ["Jaipur", "Jodhpur", "Udaipur", "Kota", "Ajmer"],
    "Telangana":        ["Hyderabad", "Warangal", "Nizamabad", "Karimnagar"],
    "Andhra Pradesh":   ["Visakhapatnam", "Vijayawada", "Guntur", "Tirupati"],
    "Kerala":           ["Thiruvananthapuram", "Kochi", "Kozhikode", "Thrissur"],
    "Punjab":           ["Ludhiana", "Amritsar", "Jalandhar", "Patiala"],
    "Madhya Pradesh":   ["Bhopal", "Indore", "Gwalior", "Jabalpur"],
    "Haryana":          ["Chandigarh", "Gurugram", "Faridabad", "Ambala"],
    "Bihar":            ["Patna", "Gaya", "Muzaffarpur", "Bhagalpur"],
}

STATE_ZONES = {
    "Maharashtra": "West", "Gujarat": "West", "Rajasthan": "West",
    "Karnataka": "South", "Tamil Nadu": "South", "Telangana": "South",
    "Andhra Pradesh": "South", "Kerala": "South",
    "Delhi": "North", "Uttar Pradesh": "North", "Punjab": "North",
    "Haryana": "North", "Madhya Pradesh": "North",
    "West Bengal": "East", "Bihar": "East",
}

CATEGORIES = {
    "Electronics":      {"products": ["Redmi Note 13 Pro", "OnePlus Nord CE4", "Samsung Galaxy M34",
                                       "boAt Airdopes 141", "Fire-Boltt Ninja", "Lenovo IdeaPad Slim 3",
                                       "HP 15s Laptop", "Realme Pad 2", "Mi 43\" Smart TV", "Noise ColorFit Pro 4"],
                          "price_range": (499, 55000), "gst": 18},
    "Fashion":          {"products": ["Manyavar Kurta", "W for Woman Saree", "Peter England Shirt",
                                       "Bata Sneakers", "Fabindia Cotton Kurta", "Allen Solly Trousers",
                                       "Aurelia Ethnic Set", "Arrow Formal Shirt", "Jockey T-Shirt", "Libas Kurti"],
                          "price_range": (199, 4999), "gst": 12},
    "Home & Kitchen":   {"products": ["Prestige Induction Cooktop", "Pigeon Non-stick Tawa",
                                       "Milton Thermosteel Flask", "Usha Mixer Grinder",
                                       "Philips Air Purifier", "Godrej Refrigerator 260L",
                                       "Havells Stand Fan", "IFB 6kg Washing Machine",
                                       "Cello Water Bottle", "Solimo Bed Sheet Set"],
                          "price_range": (299, 22000), "gst": 18},
    "Grocery & FMCG":   {"products": ["Amul Butter 500g", "Tata Tea Premium 1kg", "Surf Excel Matic 2kg",
                                       "Maggi 2-Minute Noodles 12pk", "Fortune Sunflower Oil 5L",
                                       "Colgate Strong Teeth 300g", "Dettol Antiseptic 500ml",
                                       "Parle-G Biscuits 800g", "Haldiram's Namkeen 400g", "ITC Aashirvaad Atta 10kg"],
                          "price_range": (49, 799), "gst": 5},
    "Beauty & Personal": {"products": ["Lakme 9to5 Foundation", "L'Oreal Hair Colour",
                                        "Dove Shampoo 650ml", "Himalaya Face Wash", "Nivea Body Lotion",
                                        "Mamaearth Vitamin C Serum", "WOW Skin Science Shampoo",
                                        "Biotique Bio Honey Cream", "Plum Green Tea Toner", "mCaffeine Coffee Face Scrub"],
                          "price_range": (99, 1999), "gst": 18},
    "Pharma & Health":  {"products": ["Dolo 650 Strip", "Crocin Pain Relief", "Hajmola Candy",
                                       "Revital H Capsules", "Dabur Chyawanprash 1kg",
                                       "HealthKart Whey Protein", "Glucon-D 1kg",
                                       "Patanjali Ashwagandha", "Zandu Balm 50ml", "Himalaya Liv.52"],
                          "price_range": (29, 1899), "gst": 5},
    "Sports & Fitness": {"products": ["Cosco Football", "Vector X Cricket Bat",
                                       "Nivia Gym Gloves", "Kalenji Running Shoes",
                                       "Decathlon Yoga Mat", "Yonex Badminton Racket",
                                       "Strauss Resistance Band", "Boldfit Gym Bag",
                                       "SG Cricket Pads", "Sparx Running Shoes"],
                          "price_range": (299, 4999), "gst": 12},
    "Books & Education": {"products": ["NCERT Class 12 Physics", "Arihant JEE Mathematics",
                                        "Let Us C by Yashavant Kanetkar", "Wings of Fire by Kalam",
                                        "The 3 Mistakes of My Life", "Atomic Habits Hindi",
                                        "Manorama Year Book 2024", "Oswaal CBSE Sample Papers",
                                        "R.D. Sharma Maths Class 10", "Lucent General Knowledge"],
                          "price_range": (79, 899), "gst": 0},
}

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "COD", "Net Banking", "EMI", "Wallet"]
PAYMENT_WEIGHTS  = [40, 18, 15, 12, 7, 5, 3]

ORDER_STATUSES     = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
ORDER_WEIGHTS      = [65, 12, 8, 10, 5]

TICKET_CATEGORIES  = ["Delivery Issue", "Product Defect", "Wrong Item", "Payment Failed",
                       "Return/Refund", "Account Issue", "Order Cancellation", "Quality Issue"]
TICKET_PRIORITIES  = ["Low", "Medium", "High", "Critical"]
TICKET_PRIORITY_W  = [25, 45, 20, 10]
TICKET_STATUSES    = ["Resolved", "Open", "Escalated", "Pending"]
TICKET_STATUS_W    = [68, 12, 10, 10]

FIRST_NAMES = ["Aarav","Vivaan","Aditya","Arjun","Rohit","Priya","Sneha","Anjali","Kavya","Pooja",
               "Vikram","Rahul","Suresh","Ravi","Amit","Neha","Sita","Deepa","Meera","Divya",
               "Kiran","Raj","Sanjay","Manoj","Vijay","Lakshmi","Sunita","Geeta","Anita","Rekha",
               "Harish","Naresh","Ramesh","Sunil","Arun","Usha","Mala","Sonal","Jyoti","Ritu",
               "Kartik","Nikhil","Akash","Varun","Tushar","Ritika","Shruti","Swati","Pallavi","Nisha"]

LAST_NAMES  = ["Sharma","Verma","Gupta","Singh","Kumar","Patel","Shah","Joshi","Mehta","Iyer",
               "Nair","Pillai","Reddy","Naidu","Rao","Das","Ghosh","Mukherjee","Banerjee","Sen",
               "Mishra","Pandey","Tiwari","Dubey","Yadav","Malhotra","Chopra","Khanna","Arora","Bhatia",
               "Agarwal","Garg","Goyal","Mittal","Jain","Desai","Patil","More","Jadhav","Sawant"]

AGENT_NAMES = ["Aryan Kapoor","Shreya Menon","Rohan Das","Preethi Subramaniam","Vikash Yadav",
               "Tanvi Kulkarni","Mohit Sharma","Nandini Iyer","Gaurav Tiwari","Riya Banerjee",
               "Aman Bhatt","Pallavi Reddy","Deepak Singh","Swati Joshi","Karthik Nair",
               "Ankita Patel","Nitin Malhotra","Divya Agarwal","Saurabh Gupta","Megha Pillai"]

TIER_THRESHOLDS = {"Platinum": 50000, "Gold": 20000, "Silver": 8000, "Bronze": 0}

def get_tier(spent):
    for tier, threshold in TIER_THRESHOLDS.items():
        if spent >= threshold:
            return tier
    return "Bronze"

def random_pincode(state):
    state_pin_prefix = {
        "Maharashtra": 4, "Karnataka": 5, "Tamil Nadu": 6, "Delhi": 1,
        "Uttar Pradesh": 2, "West Bengal": 7, "Gujarat": 3, "Rajasthan": 3,
        "Telangana": 5, "Andhra Pradesh": 5, "Kerala": 6, "Punjab": 1,
        "Madhya Pradesh": 4, "Haryana": 1, "Bihar": 8,
    }
    prefix = state_pin_prefix.get(state, random.randint(1, 8))
    return f"{prefix}{random.randint(10000, 99999)}"

def init_db():
    if os.path.exists(DB_PATH):
        return

    conn = get_connection()
    cur  = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS customers (
        customer_id    TEXT PRIMARY KEY,
        full_name      TEXT NOT NULL,
        email          TEXT,
        phone          TEXT,
        city           TEXT,
        state          TEXT,
        zone           TEXT,
        pincode        TEXT,
        segment        TEXT,
        tier           TEXT,
        join_date      TEXT,
        status         TEXT,
        age_group      TEXT
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id        TEXT PRIMARY KEY,
        customer_id     TEXT,
        order_date      TEXT,
        delivery_date   TEXT,
        amount          REAL,
        gst_amount      REAL,
        discount        REAL,
        final_amount    REAL,
        category        TEXT,
        product_name    TEXT,
        payment_method  TEXT,
        order_status    TEXT,
        city            TEXT,
        state           TEXT,
        zone            TEXT,
        delivery_days   INTEGER,
        is_returned     INTEGER,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE IF NOT EXISTS tickets (
        ticket_id         TEXT PRIMARY KEY,
        customer_id       TEXT,
        agent_id          TEXT,
        order_id          TEXT,
        created_date      TEXT,
        resolved_date     TEXT,
        ticket_category   TEXT,
        priority          TEXT,
        status            TEXT,
        csat_score        REAL,
        resolution_hours  REAL,
        first_response_h  REAL,
        is_repeat         INTEGER,
        state             TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
    );

    CREATE TABLE IF NOT EXISTS agents (
        agent_id    TEXT PRIMARY KEY,
        agent_name  TEXT NOT NULL,
        team        TEXT,
        shift       TEXT,
        state       TEXT,
        join_date   TEXT
    );

    CREATE TABLE IF NOT EXISTS returns (
        return_id     TEXT PRIMARY KEY,
        order_id      TEXT,
        customer_id   TEXT,
        return_date   TEXT,
        reason        TEXT,
        refund_amount REAL,
        refund_status TEXT,
        state         TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_orders_date   ON orders(order_date);
    CREATE INDEX IF NOT EXISTS idx_orders_cust   ON orders(customer_id);
    CREATE INDEX IF NOT EXISTS idx_orders_state  ON orders(state);
    CREATE INDEX IF NOT EXISTS idx_tickets_date  ON tickets(created_date);
    CREATE INDEX IF NOT EXISTS idx_tickets_agent ON tickets(agent_id);
    CREATE INDEX IF NOT EXISTS idx_returns_date  ON returns(return_date);
    """)

    random.seed(2024)
    np.random.seed(2024)

    now = datetime(2024, 12, 31)
    data_start = datetime(2022, 1, 1)   # 3 years of data
    total_days = (now - data_start).days

    # ── Agents ────────────────────────────────────────────────────────────────
    teams  = ["Tier-1 Support", "Tier-2 Technical", "Returns & Refunds", "Escalations", "Billing"]
    shifts = ["Morning (6-14)", "Afternoon (14-22)", "Night (22-6)"]
    states_list = list(STATES_CITIES.keys())

    agents = []
    for i, name in enumerate(AGENT_NAMES):
        st = random.choice(states_list)
        agents.append((
            f"AGT{i+1:03d}", name, random.choice(teams),
            random.choice(shifts), st,
            (now - timedelta(days=random.randint(90, total_days))).strftime("%Y-%m-%d")
        ))
    cur.executemany("INSERT INTO agents VALUES (?,?,?,?,?,?)", agents)

    # ── Customers ─────────────────────────────────────────────────────────────
    segments    = ["Retail", "Wholesale", "SME", "Corporate"]
    seg_weights = [55, 20, 15, 10]
    age_groups  = ["18-25", "26-35", "36-45", "46-60", "60+"]
    age_weights = [20, 35, 25, 15, 5]

    customers = []
    for i in range(2000):
        cid   = f"CUST{i+1:05d}"
        name  = f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        state = random.choice(states_list)
        city  = random.choice(STATES_CITIES[state])
        zone  = STATE_ZONES.get(state, "North")
        seg   = random.choices(segments, seg_weights)[0]
        jdate = (now - timedelta(days=random.randint(30, total_days))).strftime("%Y-%m-%d")
        status = random.choices(["Active", "Churned", "At-Risk"], [72, 14, 14])[0]
        age_group = random.choices(age_groups, age_weights)[0]
        customers.append((
            cid, name, f"{name.lower().replace(' ','.')}@gmail.com",
            f"+91-{random.randint(7000000000,9999999999)}",
            city, state, zone, random_pincode(state),
            seg, "Bronze",   # tier updated after order aggregation
            jdate, status, age_group
        ))
    cur.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", customers)

    # ── Orders ────────────────────────────────────────────────────────────────
    cust_ids = [c[0] for c in customers]
    cust_map  = {c[0]: c for c in customers}

    # Festival effect: bump volumes in Oct-Nov (Diwali), Jan (Republic Day Sale), Aug (Independence Day)
    def order_weight_for_month(m):
        festival_months = {10: 2.8, 11: 2.2, 1: 1.8, 8: 1.5, 3: 1.3, 7: 1.2}
        return festival_months.get(m, 1.0)

    orders = []
    cust_spent = {c: 0 for c in cust_ids}
    for i in range(12000):
        oid    = f"ORD{i+1:06d}"
        cid    = random.choice(cust_ids)
        c_data = cust_map[cid]
        state  = c_data[5]
        city   = c_data[4]
        zone   = c_data[6]

        # Date spanning 2022-2024 with festival weighting
        day_offset = random.randint(0, total_days)
        odate  = (now - timedelta(days=day_offset))
        while random.random() > order_weight_for_month(odate.month) / 3.0:
            day_offset = random.randint(0, total_days)
            odate = (now - timedelta(days=day_offset))

        cat_name = random.choices(list(CATEGORIES.keys()),
                                   [25, 20, 15, 12, 10, 8, 6, 4])[0]
        cat = CATEGORIES[cat_name]
        product = random.choice(cat["products"])
        lo, hi = cat["price_range"]
        base_price = round(random.uniform(lo, hi), 2)

        # Segment premium
        seg_mult = {"Corporate": 1.5, "Wholesale": 1.3, "SME": 1.1, "Retail": 1.0}
        base_price = round(base_price * seg_mult.get(c_data[8], 1.0), 2)

        gst_pct   = cat["gst"]
        gst_amt   = round(base_price * gst_pct / 100, 2)
        discount  = round(base_price * random.uniform(0, 0.25), 2)
        final_amt = round(base_price + gst_amt - discount, 2)

        pay_method   = random.choices(PAYMENT_METHODS, PAYMENT_WEIGHTS)[0]
        order_status = random.choices(ORDER_STATUSES, ORDER_WEIGHTS)[0]

        delivery_days = random.randint(2, 10) if zone in ["North", "West"] else random.randint(3, 14)
        ddate = (odate + timedelta(days=delivery_days)).strftime("%Y-%m-%d")
        is_returned = 1 if order_status == "Returned" else 0

        cust_spent[cid] += final_amt if order_status == "Delivered" else 0

        orders.append((
            oid, cid, odate.strftime("%Y-%m-%d"), ddate,
            base_price, gst_amt, discount, final_amt,
            cat_name, product, pay_method, order_status,
            city, state, zone, delivery_days, is_returned
        ))

    cur.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", orders)

    # Update tiers
    for cid, spent in cust_spent.items():
        tier = get_tier(spent)
        cur.execute("UPDATE customers SET tier=? WHERE customer_id=?", (tier, cid))

    # ── Tickets ───────────────────────────────────────────────────────────────
    agent_ids  = [a[0] for a in agents]
    order_ids  = [o[0] for o in orders]
    tickets    = []
    for i in range(5000):
        tid     = f"TKT{i+1:06d}"
        cid     = random.choice(cust_ids)
        aid     = random.choice(agent_ids)
        oid     = random.choice(order_ids)
        cat     = random.choice(TICKET_CATEGORIES)
        prio    = random.choices(TICKET_PRIORITIES, TICKET_PRIORITY_W)[0]
        status  = random.choices(TICKET_STATUSES, TICKET_STATUS_W)[0]
        cdate   = (now - timedelta(days=random.randint(0, total_days)))

        res_hrs_map = {"Low": (12, 72), "Medium": (4, 24), "High": (1, 12), "Critical": (0.5, 6)}
        lo_h, hi_h  = res_hrs_map[prio]
        res_hours   = round(random.uniform(lo_h, hi_h), 2)
        frt_hours   = round(random.uniform(0.25, res_hours * 0.5), 2)
        rdate       = (cdate + timedelta(hours=res_hours))
        csat        = max(1.0, min(5.0, round(random.gauss(3.9, 0.7), 1)))
        is_repeat   = 1 if random.random() < 0.18 else 0

        c_state = cust_map[cid][5]
        tickets.append((
            tid, cid, aid, oid,
            cdate.strftime("%Y-%m-%d"), rdate.strftime("%Y-%m-%d"),
            cat, prio, status, csat, res_hours, frt_hours, is_repeat, c_state
        ))

    cur.executemany("INSERT INTO tickets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", tickets)

    # ── Returns ───────────────────────────────────────────────────────────────
    return_reasons = ["Product Defective", "Wrong Item Delivered", "Size/Fit Issue",
                      "Not as Described", "Damaged Packaging", "Changed Mind",
                      "Better Price Available", "Delayed Delivery"]
    refund_statuses = ["Completed", "Pending", "Processing"]
    refund_weights  = [70, 18, 12]

    returned_orders = [(o[0], o[1], o[2], o[4], o[5]) for o in orders if o[16] == 1]
    returns = []
    for i, (oid, cid, odate, amt, state) in enumerate(returned_orders):
        rid = f"RET{i+1:05d}"
        rdate = (datetime.strptime(odate, "%Y-%m-%d") + timedelta(days=random.randint(1, 7))).strftime("%Y-%m-%d")
        refund_amt = round(amt * random.uniform(0.85, 1.0), 2)
        returns.append((
            rid, oid, cid, rdate,
            random.choice(return_reasons), refund_amt,
            random.choices(refund_statuses, refund_weights)[0], state
        ))

    cur.executemany("INSERT INTO returns VALUES (?,?,?,?,?,?,?,?)", returns)

    conn.commit()
    conn.close()
    print(f"Database seeded: 2000 customers, {len(orders)} orders, {len(tickets)} tickets, {len(returns)} returns.")
