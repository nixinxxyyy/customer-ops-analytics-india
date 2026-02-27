"""
India Customer Operations Analytics Dashboard
Editorial / Financial Times aesthetic — no icons, no emoji.
Fixed: state filter crash, date range 2022-2024, Gmail SMTP, YoY, cohort.
"""
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, date

from database import init_db, get_connection
from queries import (
    get_kpis, get_revenue_trend, get_state_performance, get_category_mix,
    get_payment_analysis, get_temporal_patterns, get_customer_tiers,
    get_return_analysis, get_agent_performance, get_ticket_analytics,
    get_product_performance, get_churn_risk, get_weekly_trends,
    get_weekly_csat, get_top_customers, get_zone_comparison,
    get_yoy_comparison, get_cohort_data
)
from alerts import detect_trends, build_email_html, send_email_alert
from report_generator import generate_html_report

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="India Ops Intelligence",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon=None
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:ital,wght@0,400;0,700;0,900;1,400;1,700&family=IBM+Plex+Mono:wght@300;400;500;600&family=Source+Serif+4:ital,opsz,wght@0,8..60,300;0,8..60,400;0,8..60,600;1,8..60,300;1,8..60,400&display=swap');

html, body, [class*="css"] { font-family:'Source Serif 4',Georgia,serif; background:#f5f0e8; color:#1a1a1a; }
.stApp { background:#f5f0e8; }
.main .block-container { padding:0; max-width:100%; }

div[data-testid="stSidebar"] { background:#1a1a1a; }
div[data-testid="stSidebar"] * { color:#ccc !important; }
div[data-testid="stSidebar"] .stSelectbox label,
div[data-testid="stSidebar"] .stDateInput label {
  color:#888 !important; font-family:'IBM Plex Mono',monospace !important;
  font-size:10px !important; text-transform:uppercase !important; letter-spacing:2px !important;
}
div[data-testid="stSidebar"] .stButton > button {
  background:transparent !important; border:1px solid #444 !important; color:#ccc !important;
  font-family:'IBM Plex Mono',monospace !important; font-size:11px !important;
  letter-spacing:1.5px !important; text-transform:uppercase !important;
  border-radius:0 !important; width:100% !important; padding:10px !important;
}
div[data-testid="stSidebar"] .stButton > button:hover { background:#2a2a2a !important; color:white !important; }

.masthead { background:#1a1a1a; padding:28px 48px 22px; border-bottom:3px solid #c4873a; }
.masthead-eyebrow { font-family:'IBM Plex Mono',monospace; font-size:10px; letter-spacing:4px; text-transform:uppercase; color:#c4873a; margin-bottom:8px; }
.masthead-title { font-family:'Playfair Display',serif; font-size:38px; font-weight:900; color:#f5f0e8; letter-spacing:-1px; line-height:1.05; }
.masthead-meta { font-family:'IBM Plex Mono',monospace; font-size:11px; color:#666; margin-top:8px; }

.kpi-band { background:#1a1a1a; border-top:1px solid #333; padding:0 48px; display:flex; }
.kpi-item { flex:1; padding:20px 24px 20px 0; border-right:1px solid #2a2a2a; }
.kpi-item:last-child { border-right:none; }
.kpi-label { font-family:'IBM Plex Mono',monospace; font-size:9px; letter-spacing:2.5px; text-transform:uppercase; color:#666; margin-bottom:6px; }
.kpi-value { font-family:'IBM Plex Mono',monospace; font-size:26px; font-weight:600; color:#f5f0e8; letter-spacing:-1px; line-height:1; margin-bottom:5px; }
.kpi-dp { font-size:12px; color:#4ade80; font-family:'IBM Plex Mono',monospace; }
.kpi-dn { font-size:12px; color:#f87171; font-family:'IBM Plex Mono',monospace; }
.kpi-d0 { font-size:12px; color:#888; font-family:'IBM Plex Mono',monospace; }

.section-hed { font-family:'IBM Plex Mono',monospace; font-size:9px; letter-spacing:3px; text-transform:uppercase; color:#888; border-bottom:1px solid #ccc; padding-bottom:8px; margin-bottom:16px; }

.stTabs [data-baseweb="tab-list"] { background:transparent; border-bottom:2px solid #1a1a1a; gap:0; padding:0 48px; }
.stTabs [data-baseweb="tab"] { background:transparent; border:none; border-bottom:3px solid transparent; border-radius:0; padding:12px 24px; font-family:'IBM Plex Mono',monospace; font-size:11px; letter-spacing:2px; text-transform:uppercase; color:#888; margin-bottom:-2px; }
.stTabs [aria-selected="true"] { border-bottom:3px solid #c4873a !important; color:#1a1a1a !important; background:transparent !important; }

.alert-critical { background:#fff5f5; border:1px solid #fca5a5; border-left:3px solid #dc2626; padding:14px 18px; margin:8px 0; }
.alert-warning  { background:#fffbeb; border:1px solid #fcd34d; border-left:3px solid #d97706; padding:14px 18px; margin:8px 0; }
.alert-positive { background:#f0fdf4; border:1px solid #86efac; border-left:3px solid #16a34a; padding:14px 18px; margin:8px 0; }
.alert-label { font-family:'IBM Plex Mono',monospace; font-size:9px; letter-spacing:2px; text-transform:uppercase; margin-bottom:6px; }
.alert-text  { font-size:13px; color:#333; line-height:1.5; }
.alert-pct   { font-family:'IBM Plex Mono',monospace; font-size:13px; font-weight:600; margin-top:6px; }

.stDownloadButton > button, .stButton > button {
  background:#1a1a1a !important; color:#f5f0e8 !important; border:none !important;
  border-radius:0 !important; font-family:'IBM Plex Mono',monospace !important;
  font-size:11px !important; letter-spacing:2px !important; text-transform:uppercase !important;
  padding:10px 20px !important;
}
.stDownloadButton > button:hover, .stButton > button:hover { background:#c4873a !important; }

.smtp-box { background:#f9f6f0; border:1px solid #d4cdc0; padding:20px; margin:12px 0; font-size:13px; color:#444; line-height:1.7; }
div[data-testid="stDataFrame"] th { font-family:'IBM Plex Mono',monospace !important; font-size:10px !important; text-transform:uppercase !important; letter-spacing:1.5px !important; background:#1a1a1a !important; color:#ccc !important; }
</style>
""", unsafe_allow_html=True)

# ── Init DB ────────────────────────────────────────────────────────────────────
@st.cache_resource
def setup():
    init_db()
    return True
setup()

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:24px 16px 16px">
      <div style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:3px;color:#666;text-transform:uppercase;margin-bottom:8px">India Ops Intelligence</div>
      <div style="font-family:'Playfair Display',serif;font-size:18px;font-weight:700;color:#f5f0e8;line-height:1.2">Dashboard<br>Controls</div>
      <div style="margin-top:12px;border-top:1px solid #2a2a2a;padding-top:12px;font-family:'IBM Plex Mono',monospace;font-size:10px;color:#555">v2.1 — India Market Edition<br>Data: Jan 2022 — Dec 2024</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")

    # Date range — full 2022-2024 support
    DATE_MIN = date(2022, 1, 1)
    DATE_MAX = date(2024, 12, 31)

    start_date = st.date_input("Period Start",
        value=date(2024, 1, 1),
        min_value=DATE_MIN, max_value=DATE_MAX)
    end_date = st.date_input("Period End",
        value=date(2024, 12, 31),
        min_value=DATE_MIN, max_value=DATE_MAX)

    if start_date > end_date:
        st.error("Start date must be before end date.")
        st.stop()

    conn = get_connection()
    states_list = pd.read_sql("SELECT DISTINCT state FROM orders ORDER BY state", conn)["state"].tolist()
    zones_list  = pd.read_sql("SELECT DISTINCT zone  FROM orders ORDER BY zone",  conn)["zone"].tolist()
    cats_list   = pd.read_sql("SELECT DISTINCT category FROM orders ORDER BY category", conn)["category"].tolist()
    segs_list   = pd.read_sql("SELECT DISTINCT segment FROM customers ORDER BY segment", conn)["segment"].tolist()
    conn.close()

    sel_state   = st.selectbox("State",    ["All"] + states_list)
    sel_zone    = st.selectbox("Zone",     ["All"] + zones_list)
    sel_cat     = st.selectbox("Category", ["All"] + cats_list)
    sel_segment = st.selectbox("Segment",  ["All"] + segs_list)

    st.markdown("")
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("""
    <div style="padding:16px;font-family:'IBM Plex Mono',monospace;font-size:9px;color:#444;border-top:1px solid #2a2a2a;margin-top:16px">
      Synthetic Indian e-commerce dataset<br>
      2,000 customers · 12,000+ orders<br>
      5,000 tickets · 15 states · 2022-2024
    </div>
    """, unsafe_allow_html=True)

# ── Load all data — each query gets its own cached wrapper ───────────────────
s = start_date.strftime("%Y-%m-%d")
e = end_date.strftime("%Y-%m-%d")

@st.cache_data(ttl=120)
def _c_kpis(s,e,st,zo,ca,sg):         return get_kpis(s,e,st,zo,ca,sg)
@st.cache_data(ttl=120)
def _c_trend(s,e,st,zo,ca):           return get_revenue_trend(s,e,st,zo,ca)
@st.cache_data(ttl=120)
def _c_state_p(s,e,ca):               return get_state_performance(s,e,ca)
@st.cache_data(ttl=120)
def _c_cat_mix(s,e,st,zo):            return get_category_mix(s,e,st,zo)
@st.cache_data(ttl=120)
def _c_pay(s,e,st):                   return get_payment_analysis(s,e,st)
@st.cache_data(ttl=120)
def _c_temporal(s,e):                 return get_temporal_patterns(s,e)
@st.cache_data(ttl=120)
def _c_tiers(s,e,st,sg):              return get_customer_tiers(s,e,st,sg)
@st.cache_data(ttl=120)
def _c_returns(s,e,st):               return get_return_analysis(s,e,st)
@st.cache_data(ttl=120)
def _c_agents(s,e,st):                return get_agent_performance(s,e,st)
@st.cache_data(ttl=120)
def _c_tickets(s,e,st):               return get_ticket_analytics(s,e,st)
@st.cache_data(ttl=120)
def _c_products(s,e,st,ca):           return get_product_performance(s,e,st,ca)
@st.cache_data(ttl=120)
def _c_churn(s,e,st,sg):              return get_churn_risk(s,e,st,sg)
@st.cache_data(ttl=120)
def _c_weekly_o():                    return get_weekly_trends()
@st.cache_data(ttl=120)
def _c_weekly_c():                    return get_weekly_csat()
@st.cache_data(ttl=120)
def _c_top_cust(s,e,st,sg):          return get_top_customers(s,e,st,sg)
@st.cache_data(ttl=120)
def _c_zone(s,e,ca):                  return get_zone_comparison(s,e,ca)
@st.cache_data(ttl=120)
def _c_yoy(st,ca):                    return get_yoy_comparison(st,ca)
@st.cache_data(ttl=120)
def _c_cohort(st,sg):                 return get_cohort_data(st,sg)

kpis     = _c_kpis(s, e, sel_state, sel_zone, sel_cat, sel_segment)
trend    = _c_trend(s, e, sel_state, sel_zone, sel_cat)
state_p  = _c_state_p(s, e, sel_cat)
cat_mix  = _c_cat_mix(s, e, sel_state, sel_zone)
pay_data = _c_pay(s, e, sel_state)
temporal = _c_temporal(s, e)
tiers    = _c_tiers(s, e, sel_state, sel_segment)
returns  = _c_returns(s, e, sel_state)
agents   = _c_agents(s, e, sel_state)
tickets  = _c_tickets(s, e, sel_state)
products = _c_products(s, e, sel_state, sel_cat)
churn    = _c_churn(s, e, sel_state, sel_segment)
weekly_o = _c_weekly_o()
weekly_c = _c_weekly_c()
top_cust = _c_top_cust(s, e, sel_state, sel_segment)
zone_cmp = _c_zone(s, e, sel_cat)
yoy      = _c_yoy(sel_state, sel_cat)
cohort   = _c_cohort(sel_state, sel_segment)

alerts = detect_trends(weekly_o, weekly_c)
crit_c = sum(1 for a in alerts if a["severity"] == "critical")
warn_c = sum(1 for a in alerts if a["severity"] == "warning")

# ── Plot theme ─────────────────────────────────────────────────────────────────
PLOT = dict(
    paper_bgcolor="#f5f0e8", plot_bgcolor="#f5f0e8",
    font=dict(family="'IBM Plex Mono',monospace", color="#333", size=11),
    xaxis=dict(gridcolor="#e0dbd0", linecolor="#ccc", tickfont=dict(size=10)),
    yaxis=dict(gridcolor="#e0dbd0", linecolor="#ccc", tickfont=dict(size=10)),
    margin=dict(t=32, b=32, l=8, r=8)
)
PALETTE = ["#1a1a1a","#c4873a","#5c7d6f","#8b4d6d","#4a6b8a","#7a6b3a","#6b4a3a","#3a5c6b"]

# ── Masthead ───────────────────────────────────────────────────────────────────
filter_str = " / ".join(f"{k}: {v}" for k, v in {
    "State":sel_state,"Zone":sel_zone,"Category":sel_cat,"Segment":sel_segment
}.items() if v != "All") or "All India — No filters applied"

alert_txt = (f"CRITICAL: {crit_c}" if crit_c > 0
             else f"WARNINGS: {warn_c}" if warn_c > 0 else "All Clear")
alert_col = "#f87171" if crit_c > 0 else "#facc15" if warn_c > 0 else "#4ade80"

st.markdown(f"""
<div class="masthead">
  <div class="masthead-eyebrow">Customer Operations Intelligence — India Market</div>
  <div class="masthead-title">Operations Analytics<br><span style="font-style:italic;font-weight:400;font-size:32px">Command Centre</span></div>
  <div class="masthead-meta">
    <strong style="color:#f5f0e8">{start_date.strftime('%d %b %Y')}</strong>
    <span style="color:#555"> — </span>
    <strong style="color:#f5f0e8">{end_date.strftime('%d %b %Y')}</strong>
    <span style="color:#333"> &nbsp;|&nbsp; </span>
    {filter_str}
    <span style="color:#333"> &nbsp;|&nbsp; </span>
    <span style="color:{alert_col}">{alert_txt}</span>
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI Band ───────────────────────────────────────────────────────────────────
def _d(v, suffix=""):
    cls = "kpi-dp" if v >= 0 else "kpi-dn"
    sign = "+" if v >= 0 else ""
    return f'<div class="{cls}">{sign}{v:.1f}% vs prior</div>'

st.markdown(f"""
<div class="kpi-band">
  <div class="kpi-item">
    <div class="kpi-label">Gross Merchandise Value</div>
    <div class="kpi-value">&#8377;{kpis['gmv']/100000:.1f}L</div>
    {_d(kpis['gmv_delta'])}
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Total Orders</div>
    <div class="kpi-value">{kpis['orders']:,}</div>
    {_d(kpis['orders_delta'])}
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Active Customers</div>
    <div class="kpi-value">{kpis['customers']:,}</div>
    {_d(kpis['customers_delta'])}
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Avg Order Value</div>
    <div class="kpi-value">&#8377;{kpis['aov']:,.0f}</div>
    {_d(kpis['aov_delta'])}
  </div>
  <div class="kpi-item">
    <div class="kpi-label">CSAT Score</div>
    <div class="kpi-value">{kpis['csat']:.2f}</div>
    {_d(kpis['csat_delta'])}
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Return Rate</div>
    <div class="kpi-value">{kpis['return_rate']:.1f}%</div>
    <div class="kpi-d0">6-10% benchmark</div>
  </div>
  <div class="kpi-item">
    <div class="kpi-label">Avg Delivery Days</div>
    <div class="kpi-value">{kpis['avg_delivery']:.1f}d</div>
    <div class="kpi-d0">Pan-India avg</div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Revenue & Sales", "Geographic", "Customer Intelligence",
    "Support & Tickets", "Returns & Quality", "Alerts & Reports", "Raw Data"
])

# ════════════════════════════════════════════════════════════════════
# TAB 1 — REVENUE & SALES
# ════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    st.markdown('<div style="padding:0 8px"><div class="section-hed">Revenue Trend</div></div>', unsafe_allow_html=True)

    gran = st.radio("", ["Daily","Weekly","Monthly"], horizontal=True, index=1)
    rt = trend.copy()
    if gran == "Weekly":
        rt = rt.set_index("date").resample("W").agg({"revenue":"sum","orders":"sum","discount":"sum","gst":"sum"}).reset_index()
    elif gran == "Monthly":
        rt = rt.set_index("date").resample("ME").agg({"revenue":"sum","orders":"sum","discount":"sum","gst":"sum"}).reset_index()

    fig = make_subplots(specs=[[{"secondary_y":True}]])
    fig.add_trace(go.Scatter(x=rt["date"], y=rt["revenue"], name="Revenue",
        line=dict(color="#1a1a1a",width=2), fill="tozeroy", fillcolor="rgba(26,26,26,0.06)"), secondary_y=False)
    fig.add_trace(go.Bar(x=rt["date"], y=rt["orders"], name="Orders",
        marker_color="#c4873a", opacity=0.55), secondary_y=True)
    fig.update_layout(**PLOT, height=320, hovermode="x unified",
        legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)"))
    fig.update_yaxes(tickprefix="₹", secondary_y=False)
    st.plotly_chart(fig, use_container_width=True)

    # YoY comparison
    st.markdown('<div style="padding:0 8px"><div class="section-hed">Year-on-Year Revenue Comparison (2022 / 2023 / 2024)</div></div>', unsafe_allow_html=True)
    if not yoy.empty:
        fig_yoy = go.Figure()
        colors_yoy = {"2022":"#888888","2023":"#c4873a","2024":"#1a1a1a"}
        for yr, grp in yoy.groupby("year"):
            fig_yoy.add_trace(go.Scatter(
                x=grp["month"], y=grp["revenue"],
                name=str(yr), mode="lines+markers",
                line=dict(color=colors_yoy.get(yr,"#555"), width=2),
                marker=dict(size=6)))
        fig_yoy.update_layout(**PLOT, height=300,
            legend=dict(orientation="h", bgcolor="rgba(0,0,0,0)"),
            yaxis_tickprefix="₹")
        st.plotly_chart(fig_yoy, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<div style="padding:0 8px"><div class="section-hed">Category Revenue Mix</div></div>', unsafe_allow_html=True)
        if not cat_mix.empty:
            fig_cat = px.bar(cat_mix.sort_values("revenue", ascending=True),
                x="revenue", y="category", orientation="h",
                color="category", color_discrete_sequence=PALETTE,
                text="orders", labels={"revenue":"Revenue","category":""})
            fig_cat.update_traces(texttemplate="%{text:,} orders", textposition="outside")
            fig_cat.update_layout(**PLOT, height=360, showlegend=False, xaxis_tickprefix="₹")
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.info("No data for selected filters.")

    with col2:
        st.markdown('<div style="padding:0 8px"><div class="section-hed">Day of Week Revenue Pattern</div></div>', unsafe_allow_html=True)
        if not temporal.empty:
            dow_data = temporal.groupby("dow_name")[["revenue","orders"]].mean().reindex(
                ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]).reset_index()
            fig_dow = px.bar(dow_data, x="dow_name", y="revenue",
                color_discrete_sequence=["#1a1a1a"], labels={"dow_name":"Day","revenue":"Avg Daily Revenue"})
            fig_dow.update_layout(**PLOT, height=360, yaxis_tickprefix="₹")
            st.plotly_chart(fig_dow, use_container_width=True)

    st.markdown('<div style="padding:0 8px"><div class="section-hed">Festival Month Effect — Avg Daily Revenue</div></div>', unsafe_allow_html=True)
    if not temporal.empty:
        month_data = temporal.groupby("month_name")[["revenue"]].mean().reset_index()
        order_m = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        month_data = month_data.set_index("month_name").reindex(order_m).dropna().reset_index()
        festival_m = ["Oct","Nov","Jan","Aug"]
        colors_m = ["#c4873a" if m in festival_m else "#1a1a1a" for m in month_data["month_name"]]
        fig_m = go.Figure(go.Bar(x=month_data["month_name"], y=month_data["revenue"], marker_color=colors_m))
        fig_m.update_layout(**PLOT, height=280, yaxis_tickprefix="₹")
        st.plotly_chart(fig_m, use_container_width=True)

    st.markdown('<div style="padding:0 8px"><div class="section-hed">Payment Method Breakdown</div></div>', unsafe_allow_html=True)
    if not pay_data.empty:
        c1, c2 = st.columns([2,3])
        with c1:
            fig_p = px.pie(pay_data, values="orders", names="payment_method",
                color_discrete_sequence=PALETTE, hole=0.6)
            fig_p.update_layout(**PLOT, height=280, showlegend=True, legend=dict(bgcolor="rgba(0,0,0,0)"))
            st.plotly_chart(fig_p, use_container_width=True)
        with c2:
            ps = pay_data.copy()
            ps["revenue"]     = ps["revenue"].map("₹{:,.0f}".format)
            ps["aov"]         = ps["aov"].map("₹{:,.0f}".format)
            ps["cancel_rate"] = ps["cancel_rate"].map("{:.1f}%".format)
            ps.columns = ["Method","Orders","Revenue","Avg Order","Cancel Rate"]
            st.dataframe(ps, use_container_width=True, hide_index=True, height=280)

        # Download orders data
        st.markdown("")
        conn = get_connection()
        dl_orders = pd.read_sql(f"""
            SELECT order_id, customer_id, order_date, category, product_name,
                   final_amount, payment_method, order_status, state, zone, delivery_days
            FROM orders WHERE order_date BETWEEN '{s}' AND '{e}'
            ORDER BY order_date DESC
        """, conn)
        conn.close()
        st.download_button(
            "Download Filtered Orders CSV",
            data=dl_orders.to_csv(index=False),
            file_name=f"orders_{s}_{e}.csv",
            mime="text/csv"
        )

# ════════════════════════════════════════════════════════════════════
# TAB 2 — GEOGRAPHIC
# ════════════════════════════════════════════════════════════════════
with tab2:
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    st.markdown('<div style="padding:0 8px"><div class="section-hed">State-Level Performance</div></div>', unsafe_allow_html=True)

    if not state_p.empty:
        c1, c2 = st.columns([3,2])
        with c1:
            fig_st = px.bar(state_p.sort_values("revenue", ascending=True),
                x="revenue", y="state", orientation="h",
                color="return_rate", color_continuous_scale=["#1a1a1a","#c4873a","#dc2626"],
                text="orders", labels={"revenue":"Revenue","state":"State","return_rate":"Return %"})
            fig_st.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig_st.update_layout(**PLOT, height=500, xaxis_tickprefix="₹",
                coloraxis_colorbar=dict(title="Return %", thickness=12, len=0.6))
            st.plotly_chart(fig_st, use_container_width=True)
        with c2:
            sp = state_p.copy()
            sp["revenue"]      = sp["revenue"].map("₹{:,.0f}".format)
            sp["avg_delivery"] = sp["avg_delivery"].map("{:.1f}d".format)
            sp["return_rate"]  = sp["return_rate"].map("{:.1f}%".format)
            sp = sp[["state","revenue","orders","customers","avg_delivery","return_rate"]]
            sp.columns = ["State","Revenue","Orders","Customers","Avg Delivery","Return Rate"]
            st.dataframe(sp, use_container_width=True, hide_index=True, height=500)

            st.download_button("Download State Data CSV",
                data=state_p.to_csv(index=False),
                file_name=f"state_performance_{s}_{e}.csv", mime="text/csv")

    st.markdown('<div style="padding:0 8px;margin-top:16px"><div class="section-hed">Zone Comparison</div></div>', unsafe_allow_html=True)
    if not zone_cmp.empty:
        fig_z = make_subplots(rows=1, cols=3,
            subplot_titles=["Revenue by Zone","Avg Delivery Days","Return Rate %"])
        for i, (col_, color_) in enumerate(zip(
            ["revenue","avg_delivery","return_rate"],["#1a1a1a","#c4873a","#dc2626"])):
            fig_z.add_trace(go.Bar(x=zone_cmp["zone"], y=zone_cmp[col_],
                marker_color=color_, showlegend=False), row=1, col=i+1)
        fig_z.update_layout(**PLOT, height=280)
        fig_z.update_xaxes(gridcolor="#e0dbd0", linecolor="#ccc", tickfont=dict(size=10))
        fig_z.update_yaxes(gridcolor="#e0dbd0", linecolor="#ccc", tickfont=dict(size=10))
        st.plotly_chart(fig_z, use_container_width=True)

# ════════════════════════════════════════════════════════════════════
# TAB 3 — CUSTOMER INTELLIGENCE
# ════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<div style="padding:0 8px"><div class="section-hed">Customer Tier Distribution</div></div>', unsafe_allow_html=True)
        if not tiers.empty:
            tier_sum = tiers.groupby("tier").agg(customers=("customers","sum"), revenue=("revenue","sum")).reset_index()
            tier_order = ["Platinum","Gold","Silver","Bronze"]
            tier_sum["tier"] = pd.Categorical(tier_sum["tier"], categories=tier_order, ordered=True)
            tier_sum = tier_sum.sort_values("tier")
            fig_t = make_subplots(specs=[[{"secondary_y":True}]])
            fig_t.add_trace(go.Bar(x=tier_sum["tier"], y=tier_sum["customers"], name="Customers",
                marker_color=["#1a1a1a","#c4873a","#888","#ccc"]), secondary_y=False)
            fig_t.add_trace(go.Scatter(x=tier_sum["tier"], y=tier_sum["revenue"], name="Revenue",
                mode="lines+markers", line=dict(color="#c4873a",width=2), marker=dict(size=8)), secondary_y=True)
            fig_t.update_layout(**PLOT, height=300, legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h"))
            fig_t.update_yaxes(secondary_y=True, tickprefix="₹")
            st.plotly_chart(fig_t, use_container_width=True)

    with c2:
        st.markdown('<div style="padding:0 8px"><div class="section-hed">Age Group vs Segment Matrix</div></div>', unsafe_allow_html=True)
        if not tiers.empty:
            age_seg = tiers.groupby(["age_group","segment"])["customers"].sum().reset_index()
            if not age_seg.empty:
                age_piv = age_seg.pivot(index="age_group", columns="segment", values="customers").fillna(0)
                fig_h = px.imshow(age_piv, color_continuous_scale=["#f5f0e8","#c4873a","#1a1a1a"],
                    text_auto=True, labels={"color":"Customers"})
                fig_h.update_layout(**PLOT, height=300, coloraxis_showscale=False)
                st.plotly_chart(fig_h, use_container_width=True)

    # Cohort Retention
    st.markdown('<div style="padding:0 8px"><div class="section-hed">Cohort Retention Analysis — Monthly Repeat Purchase Rate</div></div>', unsafe_allow_html=True)
    if not cohort.empty and len(cohort) > 1:
        fig_coh = px.imshow(cohort.fillna(0),
            color_continuous_scale=["#f5f0e8","#c4873a","#1a1a1a"],
            labels={"color":"Retention %"}, text_auto=True, aspect="auto")
        fig_coh.update_layout(**PLOT, height=max(300, len(cohort)*28+60), coloraxis_showscale=True,
            coloraxis_colorbar=dict(title="Retention %", thickness=12))
        fig_coh.update_traces(textfont_size=9)
        st.plotly_chart(fig_coh, use_container_width=True)
    else:
        st.info("Cohort analysis requires data across multiple months. Expand your date range to 2022-2024.")

    # Churn
    st.markdown('<div style="padding:0 8px"><div class="section-hed">Churn Risk Intelligence</div></div>', unsafe_allow_html=True)
    if not churn.empty:
        high_r = churn[churn["churn_score"] > 0.7]
        med_r  = churn[(churn["churn_score"]>0.4)&(churn["churn_score"]<=0.7)]
        low_r  = churn[churn["churn_score"]<=0.4]
        c1, c2, c3 = st.columns(3)
        for col_, label_, df_, color_, bg_ in [
            (c1,"High Risk — Act Now",    high_r,"#dc2626","#fff5f5"),
            (c2,"Medium Risk — Monitor",  med_r, "#d97706","#fffbeb"),
            (c3,"Healthy — Retain & Grow",low_r, "#16a34a","#f0fdf4"),
        ]:
            col_.markdown(f"""
            <div style="background:{bg_};border:1px solid {color_};border-left:3px solid {color_};padding:18px 20px">
              <div style="font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:2px;text-transform:uppercase;color:{color_};margin-bottom:8px">{label_}</div>
              <div style="font-family:'IBM Plex Mono',monospace;font-size:36px;font-weight:600;color:#1a1a1a;line-height:1">{len(df_):,}</div>
              <div style="font-size:12px;color:#666;margin-top:6px">customers</div>
              <div style="font-size:12px;color:#444;margin-top:8px;font-family:'IBM Plex Mono',monospace">
                Avg LTV: &#8377;{df_['lifetime_value'].mean():,.0f if len(df_)>0 else 0}<br>
                Last order: {df_['days_since_order'].mean():.0f if len(df_)>0 else 0}d ago
              </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("")
        fig_ch = px.scatter(churn, x="days_since_order", y="lifetime_value",
            color="churn_score", size="total_orders",
            color_continuous_scale=["#22c55e","#facc15","#ef4444"],
            hover_data=["full_name","city","tier","segment"],
            labels={"days_since_order":"Days Since Last Order","lifetime_value":"Lifetime Value","churn_score":"Churn Risk"})
        fig_ch.update_layout(**PLOT, height=380, coloraxis_colorbar=dict(title="Churn Risk",thickness=12))
        fig_ch.update_traces(marker=dict(opacity=0.7, line=dict(width=0)))
        st.plotly_chart(fig_ch, use_container_width=True)

        st.download_button("Download Churn Risk Data CSV",
            data=churn[["full_name","city","state","tier","segment","lifetime_value","total_orders","days_since_order","churn_score"]].to_csv(index=False),
            file_name=f"churn_risk_{s}_{e}.csv", mime="text/csv")

    st.markdown('<div style="padding:0 8px"><div class="section-hed">Top 20 Customers by Lifetime Value</div></div>', unsafe_allow_html=True)
    if not top_cust.empty:
        tc = top_cust.copy()
        tc["lifetime_value"] = tc["lifetime_value"].map("₹{:,.0f}".format)
        tc["aov"]            = tc["aov"].map("₹{:,.0f}".format)
        tc["csat_avg"]       = tc["csat_avg"].map("{:.2f}".format)
        tc.columns = ["Name","City","State","Tier","Segment","Age Group","Orders","Lifetime Value","Avg Order","CSAT"]
        st.dataframe(tc, use_container_width=True, hide_index=True)
        st.download_button("Download Top Customers CSV",
            data=top_cust.to_csv(index=False),
            file_name=f"top_customers_{s}_{e}.csv", mime="text/csv")

# ════════════════════════════════════════════════════════════════════
# TAB 4 — SUPPORT & TICKETS
# ════════════════════════════════════════════════════════════════════
with tab4:
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

    if not tickets.empty:
        t_agg = tickets.groupby("ticket_category").agg(
            total=("total","sum"), avg_res_h=("avg_res_h","mean"),
            avg_frt_h=("avg_frt_h","mean"), avg_csat=("avg_csat","mean"),
            escalated=("escalated","sum"), repeat=("repeat_contacts","sum")
        ).reset_index()

        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="padding:0 8px"><div class="section-hed">Ticket Volume by Category</div></div>', unsafe_allow_html=True)
            fig_tv = px.bar(t_agg.sort_values("total", ascending=True),
                x="total", y="ticket_category", orientation="h",
                color="avg_csat", color_continuous_scale=["#dc2626","#facc15","#22c55e"],
                range_color=[1,5], text="total", labels={"total":"Volume","ticket_category":""})
            fig_tv.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig_tv.update_layout(**PLOT, height=380, coloraxis_colorbar=dict(title="CSAT",thickness=12,len=0.6))
            st.plotly_chart(fig_tv, use_container_width=True)
        with c2:
            st.markdown('<div style="padding:0 8px"><div class="section-hed">Resolution Time vs CSAT</div></div>', unsafe_allow_html=True)
            fig_rt = px.scatter(t_agg, x="avg_res_h", y="avg_csat",
                size="total", color="ticket_category",
                color_discrete_sequence=PALETTE, text="ticket_category",
                labels={"avg_res_h":"Avg Resolution Hours","avg_csat":"Avg CSAT"})
            fig_rt.update_traces(textposition="top center", textfont_size=9)
            fig_rt.update_layout(**PLOT, height=380, showlegend=False)
            st.plotly_chart(fig_rt, use_container_width=True)

        st.markdown('<div style="padding:0 8px"><div class="section-hed">Priority Heatmap</div></div>', unsafe_allow_html=True)
        p_heat = tickets.groupby(["ticket_category","priority"])["total"].sum().reset_index()
        pivot  = p_heat.pivot(index="ticket_category", columns="priority", values="total").fillna(0)
        for p in ["Low","Medium","High","Critical"]:
            if p not in pivot.columns: pivot[p] = 0
        pivot = pivot[["Low","Medium","High","Critical"]]
        fig_ph = px.imshow(pivot, color_continuous_scale=["#f5f0e8","#c4873a","#1a1a1a"],
            text_auto=True, labels={"color":"Volume"})
        fig_ph.update_layout(**PLOT, height=340, coloraxis_showscale=False)
        st.plotly_chart(fig_ph, use_container_width=True)

        if not agents.empty:
            st.markdown('<div style="padding:0 8px"><div class="section-hed">Agent Performance Scorecard</div></div>', unsafe_allow_html=True)
            fig_ag = px.scatter(agents, x="avg_resolution_h", y="avg_csat",
                size="resolved", color="avg_csat",
                color_continuous_scale=["#dc2626","#facc15","#16a34a"], range_color=[1,5],
                text="agent_name", hover_data=["team","shift","escalated","repeat_contacts"],
                labels={"avg_resolution_h":"Avg Resolution Hours","avg_csat":"CSAT"})
            fig_ag.update_traces(textposition="top center", textfont_size=9)
            fig_ag.update_layout(**PLOT, height=400, coloraxis_colorbar=dict(title="CSAT",thickness=12))
            st.plotly_chart(fig_ag, use_container_width=True)

            ag = agents.copy()
            ag["res_rate"] = (ag["resolved"]/(ag["resolved"]+ag["escalated"].replace(0,1))*100).round(1)
            ag["avg_csat"]        = ag["avg_csat"].map("{:.2f}".format)
            ag["avg_resolution_h"]= ag["avg_resolution_h"].map("{:.1f}h".format)
            ag["avg_frt_h"]       = ag["avg_frt_h"].map("{:.1f}h".format)
            ag["res_rate"]        = ag["res_rate"].map("{:.1f}%".format)
            disp = ag[["agent_name","team","shift","total","resolved","escalated","repeat_contacts","avg_resolution_h","avg_frt_h","avg_csat","res_rate"]]
            disp.columns = ["Agent","Team","Shift","Total","Resolved","Escalated","Repeat","Avg Res","FRT","CSAT","Res Rate"]
            st.dataframe(disp, use_container_width=True, hide_index=True)

            st.download_button("Download Agent Performance CSV",
                data=agents.to_csv(index=False),
                file_name=f"agent_performance_{s}_{e}.csv", mime="text/csv")

# ════════════════════════════════════════════════════════════════════
# TAB 5 — RETURNS & QUALITY
# ════════════════════════════════════════════════════════════════════
with tab5:
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)

    if not returns.empty:
        ret_r = returns.groupby("reason").agg(returns=("returns","sum"), refund_value=("refund_value","sum")).reset_index().sort_values("returns", ascending=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="padding:0 8px"><div class="section-hed">Return Reasons</div></div>', unsafe_allow_html=True)
            fig_rr = px.bar(ret_r, x="returns", y="reason", orientation="h",
                color="refund_value", color_continuous_scale=["#f5f0e8","#c4873a","#1a1a1a"],
                text="returns", labels={"returns":"Returns","reason":""})
            fig_rr.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig_rr.update_layout(**PLOT, height=380, coloraxis_colorbar=dict(title="Refund ₹",thickness=12,len=0.6))
            st.plotly_chart(fig_rr, use_container_width=True)
        with c2:
            st.markdown('<div style="padding:0 8px"><div class="section-hed">Refund Status</div></div>', unsafe_allow_html=True)
            rs = returns.groupby("refund_status")["returns"].sum().reset_index()
            fig_rs = px.pie(rs, values="returns", names="refund_status",
                color_discrete_sequence=["#1a1a1a","#c4873a","#888"], hole=0.55)
            fig_rs.update_layout(**PLOT, height=380)
            fig_rs.update_traces(textfont_color="#1a1a1a", textinfo="label+percent+value")
            st.plotly_chart(fig_rs, use_container_width=True)

        # Return rate by state
        st.markdown('<div style="padding:0 8px"><div class="section-hed">State Return Rate vs 8% Benchmark</div></div>', unsafe_allow_html=True)
        conn = get_connection()
        ord_st = pd.read_sql(f"""SELECT state, COUNT(*) as orders FROM orders
            WHERE order_date BETWEEN '{s}' AND '{e}' AND order_status NOT IN ('Processing')
            GROUP BY state""", conn)
        conn.close()
        ret_st = returns.groupby("state")["returns"].sum().reset_index()
        retmap = ret_st.merge(ord_st, on="state")
        retmap["return_pct"] = (retmap["returns"] / retmap["orders"] * 100).round(2)
        retmap = retmap.sort_values("return_pct", ascending=False)
        fig_rm = px.bar(retmap, x="state", y="return_pct",
            color="return_pct", color_continuous_scale=["#22c55e","#facc15","#ef4444"],
            text="return_pct", labels={"state":"State","return_pct":"Return Rate %"})
        fig_rm.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_rm.add_hline(y=8, line_dash="dash", line_color="#888",
            annotation_text="8% benchmark", annotation_position="top right")
        fig_rm.update_layout(**PLOT, height=320, coloraxis_showscale=False, xaxis_tickangle=-30)
        st.plotly_chart(fig_rm, use_container_width=True)

        st.download_button("Download Returns Data CSV",
            data=returns.to_csv(index=False),
            file_name=f"returns_{s}_{e}.csv", mime="text/csv")
    else:
        st.info("No return data found for the selected filters and date range.")

    if not products.empty:
        st.markdown('<div style="padding:0 8px"><div class="section-hed">Product Return Rate Watchlist</div></div>', unsafe_allow_html=True)
        prod_r = products[products["return_rate"]>0].sort_values("return_rate", ascending=False).head(15)
        if not prod_r.empty:
            fig_pr = px.scatter(prod_r, x="orders", y="return_rate",
                size="revenue", color="category", color_discrete_sequence=PALETTE,
                text="product_name", labels={"orders":"Orders","return_rate":"Return Rate %"})
            fig_pr.update_traces(textposition="top center", textfont_size=9)
            fig_pr.add_hline(y=8, line_dash="dash", line_color="#888")
            fig_pr.update_layout(**PLOT, height=400, showlegend=True, legend=dict(bgcolor="rgba(0,0,0,0)", orientation="h"))
            st.plotly_chart(fig_pr, use_container_width=True)

# ════════════════════════════════════════════════════════════════════
# TAB 6 — ALERTS & REPORTS
# ════════════════════════════════════════════════════════════════════
with tab6:
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    c_left, c_right = st.columns([3,2])

    with c_left:
        st.markdown('<div style="padding:0 8px"><div class="section-hed">Live Trend Alerts — Automated Detection</div></div>', unsafe_allow_html=True)
        sev_order = {"critical":0,"warning":1,"positive":2,"neutral":3}
        if not alerts:
            st.markdown("""<div style="background:#f0fdf4;border:1px solid #86efac;border-left:3px solid #16a34a;padding:20px 24px">
              <div style="font-family:'IBM Plex Mono',monospace;font-size:11px;letter-spacing:1.5px;color:#16a34a;text-transform:uppercase;margin-bottom:6px">All Clear</div>
              <div style="font-size:14px;color:#333">All metrics within normal range relative to the 4-week baseline.</div>
            </div>""", unsafe_allow_html=True)
        else:
            cmap = {"critical":"#dc2626","warning":"#d97706","positive":"#16a34a","neutral":"#6b7280"}
            for a in sorted(alerts, key=lambda x: sev_order.get(x["severity"],9)):
                cls = f"alert-{a['severity']}" if a["severity"] != "neutral" else "alert-warning"
                color = cmap.get(a["severity"],"#888")
                arrow = "+" if a["direction"]=="up" else ""
                st.markdown(f"""
                <div class="{cls}" style="margin-bottom:10px">
                  <div class="alert-label" style="color:{color}">{a['severity'].upper()} — {a['metric']}</div>
                  <div class="alert-text">{a['message']}</div>
                  <div class="alert-pct" style="color:{color}">{arrow}{a['pct_change']:.1f}% vs 4-week baseline</div>
                </div>""", unsafe_allow_html=True)

        st.markdown('<div style="padding:0 0;margin-top:20px"><div class="section-hed">8-Week Operational Trends</div></div>', unsafe_allow_html=True)
        if not weekly_o.empty:
            fig_w = make_subplots(rows=2, cols=2,
                subplot_titles=["Weekly Revenue","Weekly Orders","Return Rate %","Cancel Rate %"])
            for i,(col_,r_,c_) in enumerate([("revenue",1,1),("orders",1,2),("return_rate",2,1),("cancel_rate",2,2)]):
                fig_w.add_trace(go.Scatter(
                    x=weekly_o["week"], y=weekly_o[col_], mode="lines+markers",
                    line=dict(color="#1a1a1a" if i<2 else "#c4873a", width=2), marker=dict(size=6),
                    fill="tozeroy", fillcolor="rgba(26,26,26,0.05)", showlegend=False), row=r_, col=c_)
            fig_w.update_layout(**PLOT, height=380)
            fig_w.update_xaxes(tickangle=-45, tickfont=dict(size=8), gridcolor="#e0dbd0", linecolor="#ccc")
            fig_w.update_yaxes(gridcolor="#e0dbd0", linecolor="#ccc", tickfont=dict(size=9))
            st.plotly_chart(fig_w, use_container_width=True)

    with c_right:
        # ── Report Download ────────────────────────────────────────────────
        st.markdown('<div style="padding:0 8px"><div class="section-hed">Download Analysis Report</div></div>', unsafe_allow_html=True)
        st.markdown("""<div class="smtp-box">
          A comprehensive HTML report covering all KPIs, geographic performance, customer intelligence, support metrics, and churn risk analysis. Opens in any browser and is print-ready (Ctrl+P to PDF).
        </div>""", unsafe_allow_html=True)

        report_bytes = generate_html_report(
            kpis=kpis, revenue_trend=trend, state_perf=state_p,
            category_mix=cat_mix, payment_data=pay_data,
            agent_perf=agents, ticket_data=tickets,
            churn_data=churn, start_date=s, end_date=e,
            filters={"State":sel_state,"Zone":sel_zone,"Category":sel_cat,"Segment":sel_segment}
        )
        st.download_button("Download Full Analysis Report (HTML)",
            data=report_bytes,
            file_name=f"india_ops_report_{s}_{e}.html",
            mime="text/html", use_container_width=True)

        # Download all CSVs bundle info
        st.markdown("""<div class="smtp-box" style="margin-top:10px;font-size:12px">
          Individual CSV downloads are available at the bottom of each tab (Revenue, Geographic, Customers, Support, Returns).
        </div>""", unsafe_allow_html=True)

        # ── Email Config ───────────────────────────────────────────────────
        st.markdown('<div style="padding:0 0;margin-top:16px"><div class="section-hed">Weekly Email Alert Setup</div></div>', unsafe_allow_html=True)

        st.markdown("""
        <div class="smtp-box">
          <strong>Gmail Setup (one-time):</strong><br>
          1. Go to <strong>myaccount.google.com</strong><br>
          2. Search <strong>"App passwords"</strong><br>
          3. Create an App Password for "Mail"<br>
          4. Use that 16-character code below — <em>not</em> your Gmail login password.<br><br>
          <span style="font-family:monospace;font-size:11px;color:#888">
          SMTP Host: smtp.gmail.com &nbsp;|&nbsp; Port: 587 &nbsp;|&nbsp; TLS: On
          </span>
        </div>
        """, unsafe_allow_html=True)

        with st.expander("Configure & Send Email Alert", expanded=False):
            with st.form("email_form"):
                st.markdown('<div style="font-family:monospace;font-size:11px;color:#888;margin-bottom:8px">SMTP SETTINGS</div>', unsafe_allow_html=True)
                smtp_host  = st.text_input("SMTP Host",    value="smtp.gmail.com")
                smtp_port  = st.number_input("Port",       value=587, min_value=1, max_value=65535)
                smtp_user  = st.text_input("From Email",   value="nitinsrinivasan2096@gmail.com")
                smtp_pass  = st.text_input("App Password", type="password",
                    help="Gmail App Password (16-chars). NOT your Gmail login password.")
                use_tls    = st.checkbox("Use TLS", value=True)

                st.markdown('<div style="font-family:monospace;font-size:11px;color:#888;margin-top:12px;margin-bottom:8px">RECIPIENT</div>', unsafe_allow_html=True)
                recipient  = st.text_input("To Email",     value="nitinsrinivasan2096@gmail.com")
                rec_name   = st.text_input("Recipient Name", value="Nitin")

                submitted = st.form_submit_button("Send Weekly Report Now", use_container_width=True)

            if submitted:
                if not smtp_pass.strip():
                    st.error("Enter your Gmail App Password. Go to myaccount.google.com > App passwords to create one.")
                elif not recipient.strip():
                    st.error("Enter a recipient email address.")
                else:
                    period_lbl = f"{datetime.now().strftime('%d %b %Y')} — Weekly Intelligence"
                    email_html = build_email_html(kpis, alerts, weekly_o, period_lbl, rec_name)
                    config = {"host":smtp_host,"port":int(smtp_port),"user":smtp_user,"password":smtp_pass,"use_tls":use_tls}
                    with st.spinner("Connecting to Gmail..."):
                        ok, msg = send_email_alert(config, recipient,
                            f"India Ops Weekly Intelligence — {datetime.now().strftime('%d %b %Y')}", email_html)
                    if ok:
                        st.success(f"Report sent to {recipient}")
                    else:
                        st.error(f"{msg}\n\nMake sure you used an App Password, not your Gmail login password.")

        # Email preview download
        st.markdown("")
        email_preview = build_email_html(kpis, alerts, weekly_o,
            f"{datetime.now().strftime('%d %b %Y')} — Weekly Report", "Nitin")
        st.download_button("Download Email Preview (HTML)",
            data=email_preview.encode("utf-8"),
            file_name=f"weekly_email_{datetime.now().strftime('%Y%m%d')}.html",
            mime="text/html", use_container_width=True)

# ════════════════════════════════════════════════════════════════════
# TAB 7 — RAW DATA
# ════════════════════════════════════════════════════════════════════
with tab7:
    st.markdown('<div style="height:16px"></div>', unsafe_allow_html=True)
    st.markdown('<div style="padding:0 8px"><div class="section-hed">Raw Data Explorer — Live SQL Interface</div></div>', unsafe_allow_html=True)

    c1, c2 = st.columns([2,3])
    with c1:
        table_choice = st.selectbox("Table", ["orders","customers","tickets","agents","returns"])
        row_limit    = st.slider("Row limit", 50, 2000, 500, 50)
    with c2:
        where_clause = st.text_input("SQL WHERE clause (optional)",
            placeholder="e.g.  state = 'Tamil Nadu' AND final_amount > 5000")

    conn = get_connection()
    where = f"WHERE {where_clause.strip()}" if where_clause.strip() else ""
    try:
        raw = pd.read_sql(f"SELECT * FROM {table_choice} {where} LIMIT {row_limit}", conn)
        st.markdown(f'<div style="font-family:monospace;font-size:11px;color:#888;margin-bottom:8px">{len(raw):,} rows · {table_choice}</div>', unsafe_allow_html=True)
        st.dataframe(raw, use_container_width=True, height=440)
        st.download_button(
            f"Download {table_choice}.csv",
            data=raw.to_csv(index=False),
            file_name=f"{table_choice}_{s}_{e}.csv",
            mime="text/csv"
        )
    except Exception as ex:
        st.error(f"SQL error: {ex}")
    finally:
        conn.close()

    with st.expander("Database Schema Reference"):
        conn = get_connection()
        for tbl in ["customers","orders","tickets","agents","returns"]:
            info = pd.read_sql(f"PRAGMA table_info({tbl})", conn)
            st.markdown(f'<div style="font-family:monospace;font-size:12px;font-weight:600;margin:10px 0 4px">{tbl}</div>', unsafe_allow_html=True)
            st.dataframe(info[["name","type","notnull","pk"]], use_container_width=True, hide_index=True, height=min(200, len(info)*38+38))
        conn.close()
