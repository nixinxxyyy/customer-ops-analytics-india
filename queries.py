import pandas as pd
import numpy as np
from database import get_connection
from datetime import datetime, timedelta


# ─────────────────────────────────────────────────────────────────────────────
#  Filter helpers — explicit table alias per function
# ─────────────────────────────────────────────────────────────────────────────
def _state_o(s):   return f"AND o.state = '{s}'"  if s not in ("All","",None) else ""
def _state_c(s):   return f"AND c.state = '{s}'"  if s not in ("All","",None) else ""
def _state_t(s):   return f"AND t.state = '{s}'"  if s not in ("All","",None) else ""
def _state_r(s):   return f"AND r.state = '{s}'"  if s not in ("All","",None) else ""
def _zone_o(z):    return f"AND o.zone = '{z}'"   if z not in ("All","",None) else ""
def _cat_o(c):     return f"AND o.category = '{c}'" if c not in ("All","",None) else ""
def _seg_c(s):     return f"AND c.segment = '{s}'" if s not in ("All","",None) else ""

def _delta(a, b):
    try:
        a, b = float(a), float(b)
        return ((a - b) / b * 100) if b != 0 else 0.0
    except Exception:
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  KPIs
# ─────────────────────────────────────────────────────────────────────────────
def get_kpis(start, end, state="All", zone="All", category="All", segment="All"):
    conn = get_connection()
    so = _state_o(state); zo = _zone_o(zone); co = _cat_o(category); sgc = _seg_c(segment)

    def _run(s, e):
        q = f"""
        SELECT
            COALESCE(SUM(o.final_amount),0) AS gmv,
            COALESCE(SUM(o.discount),0) AS total_discount,
            COALESCE(SUM(o.gst_amount),0) AS total_gst,
            COUNT(o.order_id) AS total_orders,
            COUNT(DISTINCT o.customer_id) AS active_customers,
            COALESCE(AVG(o.final_amount),0) AS aov,
            COALESCE(AVG(o.delivery_days),0) AS avg_delivery_days,
            SUM(CASE WHEN o.order_status='Returned'  THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS return_rate,
            SUM(CASE WHEN o.order_status='Cancelled' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS cancel_rate
        FROM orders o JOIN customers c ON o.customer_id=c.customer_id
        WHERE o.order_date BETWEEN '{s}' AND '{e}'
          AND o.order_status != 'Processing' {so}{zo}{co}{sgc}"""
        return pd.read_sql(q, conn).iloc[0]

    days = max((pd.to_datetime(end)-pd.to_datetime(start)).days, 1)
    ps = (pd.to_datetime(start)-timedelta(days=days)).strftime("%Y-%m-%d")
    pe = (pd.to_datetime(start)-timedelta(days=1)).strftime("%Y-%m-%d")
    curr, prev = _run(start, end), _run(ps, pe)

    # CSAT
    st = _state_t(state)
    q_csat = f"""SELECT COALESCE(AVG(t.csat_score),0) AS csat
        FROM tickets t JOIN customers c ON t.customer_id=c.customer_id
        WHERE t.created_date BETWEEN '{{s}}' AND '{{e}}' {st}{sgc}"""
    csat_c = pd.read_sql(q_csat.format(s=start, e=end), conn).iloc[0]["csat"]
    csat_p = pd.read_sql(q_csat.format(s=ps, e=pe),    conn).iloc[0]["csat"]

    # Resolution rate
    q_res = f"""SELECT SUM(CASE WHEN t.status='Resolved' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS rr
        FROM tickets t JOIN customers c ON t.customer_id=c.customer_id
        WHERE t.created_date BETWEEN '{{s}}' AND '{{e}}' {st}{sgc}"""
    rr_c = pd.read_sql(q_res.format(s=start, e=end), conn).iloc[0]["rr"] or 0
    rr_p = pd.read_sql(q_res.format(s=ps,    e=pe),  conn).iloc[0]["rr"] or 0

    conn.close()
    return {
        "gmv": float(curr["gmv"]),                       "gmv_delta":       _delta(curr["gmv"], prev["gmv"]),
        "orders": int(curr["total_orders"]),              "orders_delta":    _delta(curr["total_orders"], prev["total_orders"]),
        "customers": int(curr["active_customers"]),       "customers_delta": _delta(curr["active_customers"], prev["active_customers"]),
        "aov": float(curr["aov"]),                        "aov_delta":       _delta(curr["aov"], prev["aov"]),
        "csat": float(csat_c),                            "csat_delta":      _delta(csat_c, csat_p),
        "resolution": float(rr_c),                        "resolution_delta":_delta(rr_c, rr_p),
        "avg_delivery": float(curr["avg_delivery_days"]),
        "return_rate": float(curr["return_rate"] or 0),
        "cancel_rate": float(curr["cancel_rate"] or 0),
        "total_discount": float(curr["total_discount"]),
        "total_gst": float(curr["total_gst"]),
    }


def get_revenue_trend(start, end, state="All", zone="All", category="All"):
    conn = get_connection()
    q = f"""SELECT o.order_date AS date,
           SUM(o.final_amount) AS revenue, SUM(o.discount) AS discount,
           COUNT(*) AS orders, SUM(o.gst_amount) AS gst
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Cancelled','Processing')
      {_state_o(state)}{_zone_o(zone)}{_cat_o(category)}
    GROUP BY o.order_date ORDER BY o.order_date"""
    df = pd.read_sql(q, conn); conn.close()
    df["date"] = pd.to_datetime(df["date"])
    return df


def get_state_performance(start, end, category="All"):
    conn = get_connection()
    q = f"""SELECT o.state,
           SUM(o.final_amount) AS revenue, COUNT(*) AS orders,
           AVG(o.delivery_days) AS avg_delivery,
           COUNT(DISTINCT o.customer_id) AS customers,
           SUM(CASE WHEN o.order_status='Returned' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS return_rate
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Processing') {_cat_o(category)}
    GROUP BY o.state ORDER BY revenue DESC"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_category_mix(start, end, state="All", zone="All"):
    conn = get_connection()
    q = f"""SELECT o.category,
           SUM(o.final_amount) AS revenue, COUNT(*) AS orders,
           AVG(o.final_amount) AS aov, SUM(o.discount) AS discount,
           AVG(o.delivery_days) AS avg_delivery
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Cancelled','Processing')
      {_state_o(state)}{_zone_o(zone)}
    GROUP BY o.category ORDER BY revenue DESC"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_payment_analysis(start, end, state="All"):
    conn = get_connection()
    q = f"""SELECT o.payment_method, COUNT(*) AS orders,
           SUM(o.final_amount) AS revenue, AVG(o.final_amount) AS aov,
           SUM(CASE WHEN o.order_status='Cancelled' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS cancel_rate
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}' {_state_o(state)}
    GROUP BY o.payment_method ORDER BY revenue DESC"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_temporal_patterns(start, end):
    conn = get_connection()
    q = f"""SELECT o.order_date,
           strftime('%m','%w', o.order_date) AS month_dow,
           strftime('%m', o.order_date) AS month,
           strftime('%w', o.order_date) AS dow,
           SUM(o.final_amount) AS revenue, COUNT(*) AS orders
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Cancelled','Processing')
    GROUP BY o.order_date ORDER BY o.order_date"""
    # Fix: strftime can't take two format args — split into two
    q = f"""SELECT o.order_date,
           strftime('%m', o.order_date) AS month,
           strftime('%w', o.order_date) AS dow,
           SUM(o.final_amount) AS revenue, COUNT(*) AS orders
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Cancelled','Processing')
    GROUP BY o.order_date ORDER BY o.order_date"""
    df = pd.read_sql(q, conn); conn.close()
    df["date"]       = pd.to_datetime(df["order_date"])
    df["month"]      = df["month"].astype(int)
    df["dow"]        = df["dow"].astype(int)
    df["dow_name"]   = df["dow"].map({0:"Sun",1:"Mon",2:"Tue",3:"Wed",4:"Thu",5:"Fri",6:"Sat"})
    df["month_name"] = df["date"].dt.strftime("%b")
    return df


def get_customer_tiers(start, end, state="All", segment="All"):
    conn = get_connection()
    sc = _state_c(state); sgc = _seg_c(segment)
    q = f"""SELECT c.tier, c.segment, c.zone, c.age_group, c.status,
           COUNT(DISTINCT c.customer_id) AS customers,
           COALESCE(SUM(o.final_amount),0) AS revenue,
           COALESCE(AVG(o.final_amount),0) AS aov,
           COUNT(o.order_id) AS orders
    FROM customers c
    LEFT JOIN orders o ON c.customer_id=o.customer_id
      AND o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Cancelled','Processing')
    WHERE 1=1 {sc}{sgc}
    GROUP BY c.tier, c.segment, c.zone, c.age_group, c.status"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_return_analysis(start, end, state="All"):
    conn = get_connection()
    q = f"""SELECT r.reason, r.refund_status, r.state,
           COUNT(*) AS returns,
           SUM(r.refund_amount) AS refund_value,
           AVG(r.refund_amount) AS avg_refund
    FROM returns r
    WHERE r.return_date BETWEEN '{start}' AND '{end}' {_state_r(state)}
    GROUP BY r.reason, r.refund_status, r.state ORDER BY returns DESC"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_agent_performance(start, end, state="All"):
    conn = get_connection()
    q = f"""SELECT a.agent_name, a.team, a.shift,
           COUNT(t.ticket_id) AS total,
           SUM(CASE WHEN t.status='Resolved' THEN 1 ELSE 0 END) AS resolved,
           SUM(CASE WHEN t.status='Escalated' THEN 1 ELSE 0 END) AS escalated,
           AVG(t.resolution_hours) AS avg_resolution_h,
           AVG(t.first_response_h) AS avg_frt_h,
           AVG(t.csat_score) AS avg_csat,
           SUM(t.is_repeat) AS repeat_contacts
    FROM agents a JOIN tickets t ON a.agent_id=t.agent_id
    WHERE t.created_date BETWEEN '{start}' AND '{end}' {_state_t(state)}
    GROUP BY a.agent_id ORDER BY resolved DESC"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_ticket_analytics(start, end, state="All"):
    conn = get_connection()
    q = f"""SELECT t.ticket_category, t.priority,
           COUNT(*) AS total,
           AVG(t.resolution_hours) AS avg_res_h,
           AVG(t.first_response_h) AS avg_frt_h,
           AVG(t.csat_score) AS avg_csat,
           SUM(t.is_repeat) AS repeat_contacts,
           SUM(CASE WHEN t.status='Escalated' THEN 1 ELSE 0 END) AS escalated
    FROM tickets t
    WHERE t.created_date BETWEEN '{start}' AND '{end}' {_state_t(state)}
    GROUP BY t.ticket_category, t.priority ORDER BY total DESC"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_product_performance(start, end, state="All", category="All"):
    conn = get_connection()
    q = f"""SELECT o.product_name, o.category,
           COUNT(*) AS orders, SUM(o.final_amount) AS revenue,
           AVG(o.final_amount) AS aov, AVG(o.discount) AS avg_discount,
           SUM(CASE WHEN o.order_status='Returned' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS return_rate
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Processing')
      {_state_o(state)}{_cat_o(category)}
    GROUP BY o.product_name, o.category ORDER BY revenue DESC LIMIT 30"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_churn_risk(start, end, state="All", segment="All"):
    conn = get_connection()
    sc = _state_c(state); sgc = _seg_c(segment)
    q = f"""SELECT c.customer_id, c.full_name, c.city, c.state, c.tier, c.segment, c.status,
           COALESCE(SUM(o.final_amount),0) AS lifetime_value,
           COALESCE(COUNT(o.order_id),0) AS total_orders,
           COALESCE(CAST(julianday('{end}')-julianday(MAX(o.order_date)) AS INTEGER), 999) AS days_since_order,
           COALESCE(AVG(o.final_amount),0) AS avg_order_value
    FROM customers c
    LEFT JOIN orders o ON c.customer_id=o.customer_id
      AND o.order_status NOT IN ('Cancelled','Processing')
    WHERE 1=1 {sc}{sgc}
    GROUP BY c.customer_id"""
    df = pd.read_sql(q, conn); conn.close()
    np.random.seed(42)
    df["churn_score"] = (
        (df["days_since_order"].clip(0,180)/180)*0.45
        + (df["status"]=="Churned").astype(float)*0.40
        + (df["status"]=="At-Risk").astype(float)*0.25
        + (1-df["total_orders"].clip(0,10)/10)*0.10
        + np.random.uniform(0,0.05,len(df))
    ).clip(0,1)
    return df


def get_weekly_trends(weeks=8):
    conn = get_connection()
    q = """SELECT strftime('%Y-W%W', o.order_date) AS week,
           SUM(o.final_amount) AS revenue, COUNT(*) AS orders,
           AVG(o.delivery_days) AS avg_delivery,
           SUM(CASE WHEN o.order_status='Returned' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS return_rate,
           SUM(CASE WHEN o.order_status='Cancelled' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS cancel_rate
    FROM orders o GROUP BY week ORDER BY week DESC LIMIT ?"""
    df = pd.read_sql(q, conn, params=(weeks,)); conn.close()
    return df.iloc[::-1].reset_index(drop=True)


def get_weekly_csat(weeks=8):
    conn = get_connection()
    q = """SELECT strftime('%Y-W%W', t.created_date) AS week,
           AVG(t.csat_score) AS avg_csat,
           SUM(CASE WHEN t.status='Escalated' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS escalation_rate,
           COUNT(*) AS total_tickets
    FROM tickets t GROUP BY week ORDER BY week DESC LIMIT ?"""
    df = pd.read_sql(q, conn, params=(weeks,)); conn.close()
    return df.iloc[::-1].reset_index(drop=True)


def get_top_customers(start, end, state="All", segment="All", limit=20):
    conn = get_connection()
    sc = _state_c(state); sgc = _seg_c(segment)
    q = f"""SELECT c.full_name, c.city, c.state, c.tier, c.segment, c.age_group,
           COUNT(DISTINCT o.order_id) AS orders,
           SUM(o.final_amount) AS lifetime_value,
           AVG(o.final_amount) AS aov,
           COALESCE(AVG(t.csat_score),0) AS csat_avg
    FROM customers c
    JOIN orders o ON c.customer_id=o.customer_id
    LEFT JOIN tickets t ON c.customer_id=t.customer_id
      AND t.created_date BETWEEN '{start}' AND '{end}'
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Cancelled','Processing') {sc}{sgc}
    GROUP BY c.customer_id ORDER BY lifetime_value DESC LIMIT {limit}"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_zone_comparison(start, end, category="All"):
    conn = get_connection()
    q = f"""SELECT o.zone,
           SUM(o.final_amount) AS revenue, COUNT(*) AS orders,
           AVG(o.delivery_days) AS avg_delivery,
           SUM(CASE WHEN o.order_status='Returned' THEN 1.0 ELSE 0 END)*100.0/NULLIF(COUNT(*),0) AS return_rate,
           COUNT(DISTINCT o.customer_id) AS customers
    FROM orders o
    WHERE o.order_date BETWEEN '{start}' AND '{end}'
      AND o.order_status NOT IN ('Processing') {_cat_o(category)}
    GROUP BY o.zone"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_yoy_comparison(state="All", category="All"):
    conn = get_connection()
    q = f"""SELECT strftime('%Y', o.order_date) AS year,
           strftime('%m', o.order_date) AS month_num,
           strftime('%b', o.order_date) AS month,
           SUM(o.final_amount) AS revenue,
           COUNT(*) AS orders,
           AVG(o.final_amount) AS aov
    FROM orders o
    WHERE o.order_status NOT IN ('Cancelled','Processing')
      {_state_o(state)}{_cat_o(category)}
    GROUP BY year, month_num ORDER BY year, month_num"""
    df = pd.read_sql(q, conn); conn.close()
    return df


def get_cohort_data(state="All", segment="All"):
    conn = get_connection()
    sc = _state_c(state); sgc = _seg_c(segment)
    q = f"""SELECT c.customer_id,
           strftime('%Y-%m', c.join_date) AS cohort_month,
           strftime('%Y-%m', o.order_date) AS order_month
    FROM customers c
    JOIN orders o ON c.customer_id=o.customer_id
    WHERE o.order_status NOT IN ('Cancelled','Processing') {sc}{sgc}"""
    df = pd.read_sql(q, conn); conn.close()
    if df.empty:
        return pd.DataFrame()
    df["cohort_month"] = pd.to_datetime(df["cohort_month"])
    df["order_month"]  = pd.to_datetime(df["order_month"])
    df["period"] = ((df["order_month"].dt.year - df["cohort_month"].dt.year) * 12
                    + df["order_month"].dt.month - df["cohort_month"].dt.month)
    cohort_size = df.groupby("cohort_month")["customer_id"].nunique().rename("cohort_size")
    ret = df.groupby(["cohort_month","period"])["customer_id"].nunique().reset_index()
    ret = ret.join(cohort_size, on="cohort_month")
    ret["retention"] = ret["customer_id"] / ret["cohort_size"] * 100
    pivot = ret.pivot(index="cohort_month", columns="period", values="retention")
    pivot.index = pivot.index.strftime("%b %Y")
    pivot = pivot[[c for c in sorted(pivot.columns) if c <= 11]]
    pivot.columns = [f"M+{c}" for c in sorted(pivot.columns) if c <= 11]
    return pivot.round(1)
