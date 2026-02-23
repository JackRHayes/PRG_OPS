import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_weekly_report(data, recipient):
    try:
        smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.environ.get("SMTP_PORT", 587))
        smtp_user = os.environ.get("SMTP_USER", "")
        smtp_pass = os.environ.get("SMTP_PASS", "")
        msg = MIMEMultipart()
        msg["From"] = smtp_user
        msg["To"] = recipient
        msg["Subject"] = "PRG Weekly Risk Report"
        summary = data.get("summary", {})
        body = f"Total Jobs: {summary.get("total_jobs", "N/A")}"
        msg.attach(MIMEText(body, "plain"))
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, recipient, msg.as_string())
        return True
    except Exception as e:
        print(f"Email error: {e}")
        return False
