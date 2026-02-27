"""
report_generator.py  — generates a downloadable HTML analysis report
"""
import pandas as pd
from datetime import datetime


def generate_html_report(kpis, revenue_trend, state_perf, category_mix,
                          payment_data, agent_perf, ticket_data, churn_data,
                          start_date, end_date, filters: dict) -> bytes:

    # ── Summary stats ────────────────────────────────────────────────────────
    top5_states = state_perf.head(5)[["state", "revenue", "orders"]].copy()
    top5_states["revenue"] = top5_states["revenue"].map("₹{:,.0f}".format)
    top5_states_html = top5_states.to_html(index=False, border=0,
        classes="report-table", table_id="top-states")

    top5_cat = category_mix.head(5)[["category", "revenue", "orders", "aov"]].copy()
    top5_cat["revenue"] = top5_cat["revenue"].map("₹{:,.0f}".format)
    top5_cat["aov"]     = top5_cat["aov"].map("₹{:,.0f}".format)
    top5_cat_html = top5_cat.to_html(index=False, border=0,
        classes="report-table", table_id="top-cat")

    top_payment = payment_data.head(5)[["payment_method", "orders", "revenue"]].copy()
    top_payment["revenue"] = top_payment["revenue"].map("₹{:,.0f}".format)
    top_payment_html = top_payment.to_html(index=False, border=0,
        classes="report-table", table_id="top-pay")

    top_agents = agent_perf.head(5)[["agent_name", "resolved", "avg_csat", "avg_resolution_h"]].copy()
    top_agents["avg_csat"]        = top_agents["avg_csat"].map("{:.2f}".format)
    top_agents["avg_resolution_h"]= top_agents["avg_resolution_h"].map("{:.1f}h".format)
    top_agents_html = top_agents.to_html(index=False, border=0,
        classes="report-table", table_id="top-agents")

    # Revenue weekly
    rt = revenue_trend.copy()
    rt["date"] = pd.to_datetime(rt["date"])
    weekly = rt.set_index("date").resample("W").agg({"revenue": "sum", "orders": "sum"}).tail(12).reset_index()
    weekly_labels = weekly["date"].dt.strftime("%d %b").tolist()
    weekly_values = weekly["revenue"].round(0).tolist()

    # Churn risk
    high_risk = churn_data[churn_data["churn_score"] > 0.7].shape[0]
    med_risk  = churn_data[(churn_data["churn_score"] > 0.4) & (churn_data["churn_score"] <= 0.7)].shape[0]

    # Ticket breakdown
    ticket_by_cat = ticket_data.groupby("ticket_category")["total"].sum().sort_values(ascending=False)
    ticket_rows = "".join(f"<tr><td>{k}</td><td>{v:,}</td></tr>" for k, v in ticket_by_cat.items())

    delta_sign = lambda d: (f'<span style="color:#22c55e">+{d:.1f}%</span>' if d >= 0
                             else f'<span style="color:#ef4444">{d:.1f}%</span>')

    filter_summary = " | ".join(f"<strong>{k}:</strong> {v}" for k, v in filters.items() if v != "All")
    if not filter_summary:
        filter_summary = "<strong>Filters:</strong> All India — No filters applied"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Customer Operations Report — {start_date} to {end_date}</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Lora:ital,wght@0,400;0,600;0,700;1,400&family=IBM+Plex+Mono:wght@400;600&display=swap');

  * {{ box-sizing: border-box; margin: 0; padding: 0; }}

  body {{
    font-family: 'Georgia', 'Lora', serif;
    background: #faf9f7;
    color: #1a1a1a;
    line-height: 1.65;
    font-size: 14px;
  }}

  .page {{
    max-width: 960px;
    margin: 0 auto;
    padding: 60px 48px;
  }}

  /* ── Header ── */
  .report-header {{
    border-bottom: 3px solid #1a1a1a;
    padding-bottom: 28px;
    margin-bottom: 40px;
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
  }}
  .report-header h1 {{
    font-size: 28px;
    font-weight: 700;
    letter-spacing: -0.5px;
    line-height: 1.2;
  }}
  .report-header .subtitle {{
    font-size: 13px;
    color: #666;
    margin-top: 6px;
    font-style: italic;
  }}
  .report-header .meta {{
    text-align: right;
    font-size: 12px;
    color: #888;
    font-family: 'IBM Plex Mono', monospace;
  }}
  .report-header .meta strong {{ color: #1a1a1a; display: block; font-size: 13px; }}

  /* ── Section ── */
  .section {{
    margin-bottom: 44px;
    page-break-inside: avoid;
  }}
  .section-title {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 2.5px;
    color: #888;
    margin-bottom: 16px;
    padding-bottom: 8px;
    border-bottom: 1px solid #e5e5e5;
    font-family: 'IBM Plex Mono', monospace;
  }}

  /* ── KPI Grid ── */
  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1px;
    background: #1a1a1a;
    border: 1px solid #1a1a1a;
    margin-bottom: 4px;
  }}
  .kpi-cell {{
    background: #faf9f7;
    padding: 20px 16px;
  }}
  .kpi-label {{
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #888;
    font-family: 'IBM Plex Mono', monospace;
    margin-bottom: 8px;
  }}
  .kpi-value {{
    font-size: 24px;
    font-weight: 700;
    letter-spacing: -1px;
    line-height: 1;
    margin-bottom: 4px;
    font-family: 'IBM Plex Mono', monospace;
  }}
  .kpi-delta {{ font-size: 12px; }}

  /* ── Tables ── */
  .report-table {{
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
  }}
  .report-table thead tr {{
    border-bottom: 2px solid #1a1a1a;
  }}
  .report-table th {{
    text-align: left;
    padding: 8px 12px 8px 0;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #888;
    font-weight: 600;
    font-family: 'IBM Plex Mono', monospace;
  }}
  .report-table td {{
    padding: 9px 12px 9px 0;
    border-bottom: 1px solid #e8e8e8;
    vertical-align: top;
  }}
  .report-table tr:last-child td {{ border-bottom: none; }}

  /* ── Two col layout ── */
  .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 32px; }}
  .three-col {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 24px; }}

  /* ── Insight box ── */
  .insight-box {{
    background: #f0ede8;
    border-left: 3px solid #1a1a1a;
    padding: 14px 18px;
    margin: 12px 0;
    font-size: 13px;
    line-height: 1.6;
  }}

  /* ── Chart placeholder (bar via spans) ── */
  .bar-chart {{ margin: 12px 0; }}
  .bar-row {{
    display: flex;
    align-items: center;
    margin-bottom: 8px;
    gap: 10px;
  }}
  .bar-label {{
    font-size: 12px;
    width: 140px;
    flex-shrink: 0;
    font-family: 'IBM Plex Mono', monospace;
  }}
  .bar-track {{
    flex: 1;
    background: #e8e8e8;
    height: 14px;
    position: relative;
  }}
  .bar-fill {{
    background: #1a1a1a;
    height: 100%;
  }}
  .bar-val {{
    font-size: 11px;
    font-family: 'IBM Plex Mono', monospace;
    color: #444;
    white-space: nowrap;
  }}

  /* ── Risk tags ── */
  .risk-high {{ background:#fee2e2;color:#b91c1c;padding:2px 8px;border-radius:3px;font-size:11px;font-family:monospace; }}
  .risk-med  {{ background:#fef9c3;color:#92400e;padding:2px 8px;border-radius:3px;font-size:11px;font-family:monospace; }}
  .risk-low  {{ background:#dcfce7;color:#166534;padding:2px 8px;border-radius:3px;font-size:11px;font-family:monospace; }}

  /* ── Footer ── */
  .report-footer {{
    margin-top: 60px;
    padding-top: 20px;
    border-top: 1px solid #ccc;
    font-size: 11px;
    color: #888;
    display: flex;
    justify-content: space-between;
    font-family: 'IBM Plex Mono', monospace;
  }}

  @media print {{
    body {{ background: white; }}
    .page {{ padding: 20px; }}
  }}
</style>
</head>
<body>
<div class="page">

  <!-- HEADER -->
  <div class="report-header">
    <div>
      <h1>Customer Operations<br>Analytics Report</h1>
      <div class="subtitle">India Market · Comprehensive Operations Intelligence</div>
      <div style="margin-top:10px;font-size:12px;color:#888;font-family:monospace">{filter_summary}</div>
    </div>
    <div class="meta">
      <strong>{start_date} — {end_date}</strong>
      Generated {datetime.now().strftime("%d %b %Y, %H:%M")} IST<br>
      Confidential · Internal Use Only
    </div>
  </div>

  <!-- EXECUTIVE KPIs -->
  <div class="section">
    <div class="section-title">Executive Summary — Key Performance Indicators</div>
    <div class="kpi-grid">
      <div class="kpi-cell">
        <div class="kpi-label">Gross Merchandise Value</div>
        <div class="kpi-value">₹{kpis['gmv']/100000:.1f}L</div>
        <div class="kpi-delta">{delta_sign(kpis['gmv_delta'])} vs prior period</div>
      </div>
      <div class="kpi-cell">
        <div class="kpi-label">Total Orders</div>
        <div class="kpi-value">{kpis['orders']:,}</div>
        <div class="kpi-delta">{delta_sign(kpis['orders_delta'])} vs prior period</div>
      </div>
      <div class="kpi-cell">
        <div class="kpi-label">Active Customers</div>
        <div class="kpi-value">{kpis['customers']:,}</div>
        <div class="kpi-delta">{delta_sign(kpis['customers_delta'])} vs prior period</div>
      </div>
      <div class="kpi-cell">
        <div class="kpi-label">Avg Order Value</div>
        <div class="kpi-value">₹{kpis['aov']:,.0f}</div>
        <div class="kpi-delta">{delta_sign(kpis['aov_delta'])} vs prior period</div>
      </div>
      <div class="kpi-cell">
        <div class="kpi-label">CSAT Score</div>
        <div class="kpi-value">{kpis['csat']:.2f}</div>
        <div class="kpi-delta">{delta_sign(kpis['csat_delta'])} vs prior period</div>
      </div>
      <div class="kpi-cell">
        <div class="kpi-label">Resolution Rate</div>
        <div class="kpi-value">{kpis['resolution']:.0f}%</div>
        <div class="kpi-delta">{delta_sign(kpis['resolution_delta'])} vs prior period</div>
      </div>
      <div class="kpi-cell">
        <div class="kpi-label">Return Rate</div>
        <div class="kpi-value">{kpis['return_rate']:.1f}%</div>
        <div class="kpi-delta">Delivered orders</div>
      </div>
      <div class="kpi-cell">
        <div class="kpi-label">Avg Delivery Days</div>
        <div class="kpi-value">{kpis['avg_delivery']:.1f}d</div>
        <div class="kpi-delta">Pan-India average</div>
      </div>
    </div>
    <div style="margin-top:8px;font-size:11px;color:#999;font-family:monospace">
      Total GST Collected: ₹{kpis['total_gst']:,.0f} &nbsp;|&nbsp;
      Total Discounts Given: ₹{kpis['total_discount']:,.0f} &nbsp;|&nbsp;
      Cancellation Rate: {kpis['cancel_rate']:.1f}%
    </div>
  </div>

  <!-- STATE PERFORMANCE -->
  <div class="section">
    <div class="section-title">Geographic Performance — Top 5 States</div>
    <div class="two-col">
      <div>
        {top5_states_html}
      </div>
      <div>
        <div class="bar-chart">
          {"".join(
            f'<div class="bar-row"><div class="bar-label">{row["state"]}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{row["revenue"]/state_perf["revenue"].max()*100:.1f}%"></div></div>'
            f'<div class="bar-val">₹{row["revenue"]/100000:.1f}L</div></div>'
            for _, row in state_perf.head(8).iterrows()
          )}
        </div>
      </div>
    </div>
  </div>

  <!-- CATEGORY MIX -->
  <div class="section">
    <div class="section-title">Category Performance</div>
    <div class="two-col">
      <div>
        {top5_cat_html}
      </div>
      <div>
        <div class="bar-chart">
          {"".join(
            f'<div class="bar-row"><div class="bar-label">{row["category"]}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{row["revenue"]/category_mix["revenue"].max()*100:.1f}%"></div></div>'
            f'<div class="bar-val">{row["orders"]:,} orders</div></div>'
            for _, row in category_mix.iterrows()
          )}
        </div>
      </div>
    </div>
  </div>

  <!-- PAYMENT & TICKETS -->
  <div class="section">
    <div class="section-title">Payment Methods & Support Tickets</div>
    <div class="two-col">
      <div>
        <div style="font-size:12px;font-weight:600;margin-bottom:10px;text-transform:uppercase;letter-spacing:1px;color:#444">Payment Methods</div>
        {top_payment_html}
      </div>
      <div>
        <div style="font-size:12px;font-weight:600;margin-bottom:10px;text-transform:uppercase;letter-spacing:1px;color:#444">Tickets by Category</div>
        <table class="report-table">
          <thead><tr><th>Category</th><th>Volume</th></tr></thead>
          <tbody>{ticket_rows}</tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- AGENT PERFORMANCE -->
  <div class="section">
    <div class="section-title">Top Support Agents</div>
    {top_agents_html}
  </div>

  <!-- CHURN RISK -->
  <div class="section">
    <div class="section-title">Customer Churn Risk Summary</div>
    <div class="three-col">
      <div class="insight-box" style="border-left-color:#ef4444">
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:1.5px;color:#888;margin-bottom:6px">High Risk (&gt;70%)</div>
        <div style="font-size:32px;font-weight:700;font-family:monospace;color:#ef4444">{high_risk}</div>
        <div style="font-size:12px;color:#666;margin-top:4px">customers at immediate risk</div>
      </div>
      <div class="insight-box" style="border-left-color:#f59e0b">
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:1.5px;color:#888;margin-bottom:6px">Medium Risk (40-70%)</div>
        <div style="font-size:32px;font-weight:700;font-family:monospace;color:#f59e0b">{med_risk}</div>
        <div style="font-size:12px;color:#666;margin-top:4px">customers requiring attention</div>
      </div>
      <div class="insight-box" style="border-left-color:#22c55e">
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:1.5px;color:#888;margin-bottom:6px">Low Risk (&lt;40%)</div>
        <div style="font-size:32px;font-weight:700;font-family:monospace;color:#22c55e">{len(churn_data) - high_risk - med_risk}</div>
        <div style="font-size:12px;color:#666;margin-top:4px">customers in good standing</div>
      </div>
    </div>
  </div>

  <!-- INSIGHTS -->
  <div class="section">
    <div class="section-title">Analytical Observations</div>
    <div class="insight-box">
      <strong>Revenue Concentration:</strong> The top 3 states account for
      {state_perf.head(3)["revenue"].sum() / state_perf["revenue"].sum() * 100:.1f}% of total GMV,
      suggesting significant geographic concentration. Expansion into Tier-2 cities within
      {state_perf.iloc[3]["state"] if len(state_perf) > 3 else "other states"} could diversify revenue risk.
    </div>
    <div class="insight-box">
      <strong>UPI Dominance:</strong> Digital payment adoption (UPI + Wallet) indicates a
      digitally-savvy customer base. COD orders carry higher return risk and operational cost —
      targeted incentives to shift COD users to UPI can improve unit economics.
    </div>
    <div class="insight-box">
      <strong>Return Rate Analysis:</strong> A {kpis['return_rate']:.1f}% return rate is
      {"above" if kpis['return_rate'] > 8 else "within"} the Indian e-commerce benchmark of 6-10%.
      Cross-referencing return reasons with specific product categories can identify quality issues early.
    </div>
    <div class="insight-box">
      <strong>Churn Risk:</strong> {high_risk} high-risk customers represent potential revenue leakage.
      Immediate re-engagement campaigns (personalised offers, loyalty rewards) for Platinum/Gold tier
      high-risk customers should be prioritised given their higher lifetime value.
    </div>
  </div>

  <!-- FOOTER -->
  <div class="report-footer">
    <span>Customer Operations Analytics Dashboard · India Operations</span>
    <span>Report Period: {start_date} — {end_date}</span>
    <span>Page 1 of 1</span>
  </div>

</div>
</body>
</html>"""
    return html.encode("utf-8")
