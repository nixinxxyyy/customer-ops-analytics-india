"""
alerts.py — Trend detection engine + HTML email generator
"""
import pandas as pd
import numpy as np
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# ─────────────────────────────────────────────────────────────────────────────
#  Trend Detection
# ─────────────────────────────────────────────────────────────────────────────
def detect_trends(weekly_ops: pd.DataFrame, weekly_csat: pd.DataFrame) -> list[dict]:
    """
    Analyse last 2 weeks vs prior 4-week average.
    Returns a list of alert dicts with keys:
        metric, current, baseline, pct_change, direction, severity, message
    """
    alerts = []
    if len(weekly_ops) < 4:
        return alerts

    baseline_ops  = weekly_ops.iloc[-6:-2] if len(weekly_ops) >= 6 else weekly_ops.iloc[:-2]
    last_week_ops = weekly_ops.iloc[-1]
    prev_week_ops = weekly_ops.iloc[-2]

    def check(metric, curr_val, base_val, label, unit="", higher_better=True, threshold=8.0):
        if base_val == 0:
            return
        pct = (curr_val - base_val) / base_val * 100
        direction = "up" if pct > 0 else "down"

        if higher_better:
            severity = "critical" if pct < -15 else "warning" if pct < -threshold else "positive" if pct > threshold else "neutral"
        else:
            severity = "critical" if pct > 15 else "warning" if pct > threshold else "positive" if pct < -threshold else "neutral"

        if abs(pct) >= threshold:
            alerts.append({
                "metric": label, "current": curr_val, "baseline": base_val,
                "pct_change": pct, "direction": direction, "severity": severity,
                "unit": unit,
                "message": _make_message(label, pct, curr_val, base_val, unit, higher_better)
            })

    base_rev   = baseline_ops["revenue"].mean()
    base_ord   = baseline_ops["orders"].mean()
    base_ret   = baseline_ops["return_rate"].mean()
    base_can   = baseline_ops["cancel_rate"].mean()
    base_del   = baseline_ops["avg_delivery"].mean()

    check("Weekly Revenue",       last_week_ops["revenue"],      base_rev,  "Revenue",          "₹", True)
    check("Weekly Orders",        last_week_ops["orders"],       base_ord,  "Orders",            "",  True)
    check("Return Rate",          last_week_ops["return_rate"],  base_ret,  "Return Rate",       "%", False, 5.0)
    check("Cancellation Rate",    last_week_ops["cancel_rate"],  base_can,  "Cancellation Rate", "%", False, 5.0)
    check("Avg Delivery Days",    last_week_ops["avg_delivery"], base_del,  "Delivery Days",     "d", False, 5.0)

    if len(weekly_csat) >= 4:
        base_csat = weekly_csat.iloc[-5:-1]["avg_csat"].mean() if len(weekly_csat) >= 5 else weekly_csat.iloc[:-1]["avg_csat"].mean()
        last_csat = weekly_csat.iloc[-1]["avg_csat"]
        base_esc  = weekly_csat.iloc[-5:-1]["escalation_rate"].mean() if len(weekly_csat) >= 5 else weekly_csat.iloc[:-1]["escalation_rate"].mean()
        last_esc  = weekly_csat.iloc[-1]["escalation_rate"]
        check("CSAT Score",        last_csat, base_csat, "CSAT Score",       "", True,  3.0)
        check("Escalation Rate",   last_esc,  base_esc,  "Escalation Rate",  "%", False, 5.0)

    return alerts


def _make_message(label, pct, curr, base, unit, higher_better):
    direction = "increased" if pct > 0 else "decreased"
    good = (pct > 0 and higher_better) or (pct < 0 and not higher_better)
    sentiment = "improving" if good else "concerning"
    if unit == "₹":
        curr_fmt, base_fmt = f"₹{curr:,.0f}", f"₹{base:,.0f}"
    elif unit == "%":
        curr_fmt, base_fmt = f"{curr:.1f}%", f"{base:.1f}%"
    elif unit == "d":
        curr_fmt, base_fmt = f"{curr:.1f} days", f"{base:.1f} days"
    else:
        curr_fmt, base_fmt = f"{curr:,.1f}", f"{base:,.1f}"
    return f"{label} has {direction} by {abs(pct):.1f}% (current: {curr_fmt}, 4-week avg: {base_fmt}). Trend is {sentiment}."


# ─────────────────────────────────────────────────────────────────────────────
#  HTML Email Builder
# ─────────────────────────────────────────────────────────────────────────────
SEVERITY_STYLES = {
    "critical": ("background:#fef2f2;border-left:4px solid #dc2626;", "#dc2626", "CRITICAL"),
    "warning":  ("background:#fffbeb;border-left:4px solid #d97706;", "#d97706", "WARNING"),
    "positive": ("background:#f0fdf4;border-left:4px solid #16a34a;", "#16a34a", "POSITIVE"),
    "neutral":  ("background:#f8f9fa;border-left:4px solid #6b7280;", "#6b7280", "INFO"),
}

def build_email_html(kpis: dict, alerts: list[dict], weekly_ops: pd.DataFrame,
                     period_label: str, recipient_name: str = "Team") -> str:

    # Sort alerts by severity priority
    sev_order = {"critical": 0, "warning": 1, "positive": 2, "neutral": 3}
    alerts_sorted = sorted(alerts, key=lambda x: sev_order.get(x["severity"], 9))

    alert_blocks = ""
    for a in alerts_sorted:
        style, color, tag = SEVERITY_STYLES.get(a["severity"], SEVERITY_STYLES["neutral"])
        arrow = "↑" if a["direction"] == "up" else "↓"
        pct_color = color
        alert_blocks += f"""
        <tr>
          <td style="padding:12px 16px">
            <div style="{style}padding:14px 16px;margin-bottom:0">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                <strong style="font-size:14px;color:#111">{a['metric']}</strong>
                <span style="background:{color};color:white;padding:2px 8px;border-radius:3px;font-size:11px;font-family:monospace;font-weight:600">{tag}</span>
              </div>
              <div style="font-size:13px;color:#444;line-height:1.5">{a['message']}</div>
              <div style="margin-top:8px;font-size:13px;font-family:monospace;color:{pct_color};font-weight:700">
                {arrow} {abs(a['pct_change']):.1f}% vs 4-week baseline
              </div>
            </div>
          </td>
        </tr>"""

    if not alert_blocks:
        alert_blocks = """<tr><td style="padding:20px 16px;color:#666;font-style:italic;font-size:14px">
            No significant trend deviations detected this week. All metrics within normal range.
        </td></tr>"""

    # Trend sparkline (text-based for email compatibility)
    recent = weekly_ops.tail(6)
    sparkline = ""
    if len(recent) > 1:
        max_r = recent["revenue"].max()
        for _, row in recent.iterrows():
            h = int(row["revenue"] / max_r * 24) if max_r > 0 else 4
            sparkline += f'<div style="display:inline-block;width:16px;height:{max(2,h)}px;background:#1a1a1a;margin:0 1px;vertical-align:bottom"></div>'

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#f4f4f0;font-family:Georgia,serif">
<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f0;padding:40px 0">
  <tr><td align="center">
  <table width="620" cellpadding="0" cellspacing="0" style="background:white;border:1px solid #ddd">

    <!-- HEADER -->
    <tr>
      <td style="background:#1a1a1a;padding:32px 36px">
        <div style="color:#999;font-size:10px;letter-spacing:3px;text-transform:uppercase;font-family:monospace;margin-bottom:8px">Weekly Intelligence Report</div>
        <div style="color:white;font-size:24px;font-weight:700;letter-spacing:-0.5px">Customer Operations<br>Analytics</div>
        <div style="color:#888;font-size:12px;margin-top:8px;font-family:monospace">{period_label}</div>
      </td>
    </tr>

    <!-- GREETING -->
    <tr>
      <td style="padding:28px 36px 16px">
        <p style="font-size:15px;color:#222;line-height:1.6;margin:0">
          Dear {recipient_name},<br><br>
          Here is your automated weekly operations intelligence summary for the India market.
          This report highlights significant trend deviations that require your attention.
        </p>
      </td>
    </tr>

    <!-- KPI STRIP -->
    <tr>
      <td style="padding:0 36px 24px">
        <table width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #e5e5e5">
          <tr>
            <td style="padding:16px;border-right:1px solid #e5e5e5;text-align:center">
              <div style="font-size:10px;color:#888;letter-spacing:1.5px;text-transform:uppercase;font-family:monospace;margin-bottom:6px">GMV</div>
              <div style="font-size:22px;font-weight:700;font-family:monospace;color:#111">₹{kpis['gmv']/100000:.1f}L</div>
              <div style="font-size:11px;color:{'#22c55e' if kpis['gmv_delta']>=0 else '#ef4444'};font-family:monospace">
                {'↑' if kpis['gmv_delta']>=0 else '↓'}{abs(kpis['gmv_delta']):.1f}%
              </div>
            </td>
            <td style="padding:16px;border-right:1px solid #e5e5e5;text-align:center">
              <div style="font-size:10px;color:#888;letter-spacing:1.5px;text-transform:uppercase;font-family:monospace;margin-bottom:6px">Orders</div>
              <div style="font-size:22px;font-weight:700;font-family:monospace;color:#111">{kpis['orders']:,}</div>
              <div style="font-size:11px;color:{'#22c55e' if kpis['orders_delta']>=0 else '#ef4444'};font-family:monospace">
                {'↑' if kpis['orders_delta']>=0 else '↓'}{abs(kpis['orders_delta']):.1f}%
              </div>
            </td>
            <td style="padding:16px;border-right:1px solid #e5e5e5;text-align:center">
              <div style="font-size:10px;color:#888;letter-spacing:1.5px;text-transform:uppercase;font-family:monospace;margin-bottom:6px">CSAT</div>
              <div style="font-size:22px;font-weight:700;font-family:monospace;color:#111">{kpis['csat']:.2f}</div>
              <div style="font-size:11px;color:{'#22c55e' if kpis['csat_delta']>=0 else '#ef4444'};font-family:monospace">
                {'↑' if kpis['csat_delta']>=0 else '↓'}{abs(kpis['csat_delta']):.1f}%
              </div>
            </td>
            <td style="padding:16px;text-align:center">
              <div style="font-size:10px;color:#888;letter-spacing:1.5px;text-transform:uppercase;font-family:monospace;margin-bottom:6px">Return Rate</div>
              <div style="font-size:22px;font-weight:700;font-family:monospace;color:#111">{kpis['return_rate']:.1f}%</div>
              <div style="font-size:11px;color:#888;font-family:monospace">vs 6-10% benchmark</div>
            </td>
          </tr>
        </table>
      </td>
    </tr>

    <!-- TREND SPARKLINE -->
    <tr>
      <td style="padding:0 36px 24px">
        <div style="font-size:10px;color:#888;letter-spacing:2px;text-transform:uppercase;font-family:monospace;margin-bottom:10px">Revenue Trend — Last 6 Weeks</div>
        <div style="display:flex;align-items:flex-end;gap:2px;height:28px">{sparkline}</div>
      </td>
    </tr>

    <!-- ALERTS SECTION -->
    <tr>
      <td style="padding:0 36px">
        <div style="font-size:10px;color:#888;letter-spacing:2px;text-transform:uppercase;font-family:monospace;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #e5e5e5">
          Trend Alerts &amp; Anomalies
        </div>
      </td>
    </tr>
    <tr>
      <td style="padding:0 36px 24px">
        <table width="100%" cellpadding="0" cellspacing="0">
          {alert_blocks}
        </table>
      </td>
    </tr>

    <!-- RECOMMENDATIONS -->
    <tr>
      <td style="padding:0 36px 28px">
        <div style="background:#f9f9f7;border:1px solid #e5e5e5;padding:20px">
          <div style="font-size:10px;color:#888;letter-spacing:2px;text-transform:uppercase;font-family:monospace;margin-bottom:12px">Recommended Actions</div>
          <ul style="margin:0;padding-left:18px;font-size:13px;color:#333;line-height:1.9">
            {"".join(
              f'<li>{_get_recommendation(a)}</li>'
              for a in alerts_sorted if a["severity"] in ("critical", "warning")
            ) or '<li>Continue monitoring. No critical actions required this week.</li>'}
          </ul>
        </div>
      </td>
    </tr>

    <!-- FOOTER -->
    <tr>
      <td style="background:#1a1a1a;padding:20px 36px">
        <table width="100%" cellpadding="0" cellspacing="0">
          <tr>
            <td style="color:#888;font-size:11px;font-family:monospace">Customer Operations Analytics · India Market</td>
            <td align="right" style="color:#666;font-size:11px;font-family:monospace">Auto-generated · {datetime.now().strftime('%d %b %Y')}</td>
          </tr>
        </table>
      </td>
    </tr>

  </table>
  </td></tr>
</table>
</body>
</html>"""
    return html


def _get_recommendation(alert: dict) -> str:
    recs = {
        "Revenue":          "Investigate order pipeline for the current week. Check if promotional campaigns are active and consider flash sale or targeted discount to boost GMV.",
        "Orders":           "Review acquisition channels. Check if there are any technical issues with checkout flow or if competitor activity is causing volume drop.",
        "Return Rate":      "Audit product listings and QC for top-returned categories. Consider tightening return windows for Electronics and Fashion. Review courier partner performance.",
        "Cancellation Rate":"Identify if cancellations are pre-delivery or post-payment. Review payment gateway uptime and implement cancellation deterrence at checkout.",
        "Delivery Days":    "Escalate to logistics partners for the affected states. Review last-mile carrier SLAs and consider activating backup delivery partners.",
        "CSAT Score":       "Pull CSAT verbatims for the past week. Schedule urgent CX review with Tier-2 support team. Check for any product-specific complaint spikes.",
        "Escalation Rate":  "Review Tier-1 agent training for the categories driving escalations. Check if new product launches are causing knowledge gaps.",
    }
    for key, rec in recs.items():
        if key.lower() in alert["metric"].lower():
            return rec
    return f"Review {alert['metric']} trend and investigate root cause with the respective team."


# ─────────────────────────────────────────────────────────────────────────────
#  SMTP Sender
# ─────────────────────────────────────────────────────────────────────────────
def send_email_alert(smtp_config: dict, recipient_email: str, subject: str, html_body: str) -> tuple[bool, str]:
    """
    smtp_config keys: host, port, user, password, use_tls (bool)
    Returns (success: bool, message: str)
    """
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = smtp_config["user"]
        msg["To"]      = recipient_email
        msg.attach(MIMEText(html_body, "html"))

        if smtp_config.get("use_tls", True):
            server = smtplib.SMTP(smtp_config["host"], smtp_config["port"])
            server.ehlo()
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(smtp_config["host"], smtp_config["port"])

        server.login(smtp_config["user"], smtp_config["password"])
        server.sendmail(smtp_config["user"], recipient_email, msg.as_string())
        server.quit()
        return True, "Email sent successfully."
    except Exception as e:
        return False, f"Failed to send email: {str(e)}"
