# Customer Operations Analytics Dashboard — India Market

A professional analytics dashboard built with Python, Streamlit, Pandas & SQLite.

## Features
- 7 analytics tabs: Revenue, Geographic, Customer Intelligence, Support, Returns, Alerts, Raw Data
- 3 years of Indian e-commerce data (2022–2024)
- 15 Indian states, real product names, GST tracking, UPI/COD payments
- Automated email alerts with Gmail SMTP
- Downloadable HTML reports and CSV exports
- Year-on-Year comparison and Cohort Retention analysis

## Tech Stack
- Python 3.10+
- Streamlit
- Pandas
- SQLite (via sqlite3)
- Plotly

## Run Locally
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Project Structure
```
india_ops_dashboard/
├── app.py               # Main dashboard UI
├── database.py          # Schema + seed data (2022-2024)
├── queries.py           # All SQL query functions
├── alerts.py            # Trend detection + email HTML
├── report_generator.py  # Downloadable HTML report
└── requirements.txt
```
