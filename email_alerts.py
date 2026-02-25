import smtplib
import os
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')


def _load_smtp_creds():
    try:
        with open(CONFIG_FILE) as f:
            cfg = json.load(f)
        return cfg.get('smtp_user', ''), cfg.get('smtp_pass', '')
    except Exception:
        return '', ''


def send_weekly_report(data, recipient):
    try:
        smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.environ.get("SMTP_PORT", 587))
        cfg_user, cfg_pass = _load_smtp_creds()
        smtp_user = cfg_user or os.environ.get("SMTP_USER", "")
        smtp_pass = cfg_pass or os.environ.get("SMTP_PASS", "")

        if not smtp_user or not smtp_pass:
            print("Email error: SMTP credentials not configured")
            return False

        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = recipient
        msg["Subject"] = "PRG Weekly Risk Report"
        summary = data.get("summary", {})
        body = f"Total Jobs: {summary.get('total_jobs', 'N/A')}"
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipient, msg.as_string())
        return True
    except Exception as e:
        print(f"Email error: {e}")
        return False
