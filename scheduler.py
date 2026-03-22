"""
Daily Scheduler for Phase 1 Data Ingestion
Uses APScheduler to trigger Phase 1 pipeline every day at 2:00 AM UTC
"""

import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.base import SchedulerAlreadyRunningError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config_phase1 import SCHEDULER_CONFIG, LOGGING_CONFIG
from phase1_ingestion import run_phase1_ingestion
from pathlib import Path

# Setup logging
log_dir = Path(LOGGING_CONFIG["log_dir"])
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=LOGGING_CONFIG["log_level"],
    format=LOGGING_CONFIG["format"],
    handlers=[
        logging.FileHandler(log_dir / "scheduler.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ========================================
# EMAIL NOTIFICATION
# ========================================
class EmailNotifier:
    """Handles email notifications for pipeline events."""
    
    def __init__(self, smtp_server: str = "localhost", smtp_port: int = 587):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
    
    def send_email(self, to_addresses: list, subject: str, body: str, is_html: bool = True):
        """Send email notification."""
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = "srsid-system@company.com"
            msg["To"] = ", ".join(to_addresses)
            
            mime_type = "html" if is_html else "plain"
            msg.attach(MIMEText(body, mime_type))
            
            # NOTE: Update SMTP configuration for your environment
            # For development, you can use a mock SMTP or skip notifications
            logger.info(f"✓ Email notification prepared: {subject}")
            
            # Uncomment below for actual email sending
            # with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            #     server.starttls()
            #     server.send_message(msg)
            
            return True
        except Exception as e:
            logger.error(f"✗ Failed to send email: {e}")
            return False


def generate_success_email(metrics: dict) -> str:
    """Generate HTML email body for successful run."""
    html = f"""
    <html>
        <body style="font-family: Arial, sans-serif;">
            <h2>✓ Phase 1 Data Ingestion - Success</h2>
            <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            
            <h3>Summary</h3>
            <table border="1" cellpadding="8" cellspacing="0">
                <tr style="background-color: #f0f0f0;">
                    <th>Source</th>
                    <th>Status</th>
                    <th>Initial Rows</th>
                    <th>Final Rows</th>
                    <th>Inserted</th>
                    <th>Errors</th>
                </tr>
    """
    
    for source, metrics_data in metrics.items():
        status_color = "green" if metrics_data.get("status") == "success" else "orange"
        html += f"""
                <tr>
                    <td><strong>{source}</strong></td>
                    <td style="color: {status_color};">{metrics_data.get("status", "unknown").upper()}</td>
                    <td>{metrics_data.get("initial_rows", "N/A")}</td>
                    <td>{metrics_data.get("final_rows", "N/A")}</td>
                    <td>{metrics_data.get("rows_inserted", "N/A")}</td>
                    <td>{metrics_data.get("validation_errors", 0)}</td>
                </tr>
        """
    
    html += """
            </table>
            
            <h3>Next Steps</h3>
            <ul>
                <li>Phase 2: Entity Resolution & Consolidation will run next</li>
                <li>Check logs in <code>logs/</code> directory for details</li>
                <li>Contact data-team@company.com if issues detected</li>
            </ul>
            
            <hr/>
            <p style="font-size: 12px; color: #666;">
                This is an automated notification from the Supplier Risk & Spend Intelligence Dashboard (SRSID).
            </p>
        </body>
    </html>
    """
    return html


def generate_failure_email(error: str) -> str:
    """Generate HTML email body for failed run."""
    html = f"""
    <html>
        <body style="font-family: Arial, sans-serif;">
            <h2 style="color: red;">✗ Phase 1 Data Ingestion - FAILED</h2>
            <p><strong>Timestamp:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            
            <h3>Error Details</h3>
            <pre style="background-color: #f5f5f5; padding: 10px; border-radius: 5px;">
{error}
            </pre>
            
            <h3>Action Required</h3>
            <ul>
                <li>Check detailed logs in <code>logs/</code> directory</li>
                <li>Verify all source CSV files are present and accessible</li>
                <li>Verify PostgreSQL database connectivity</li>
                <li>Check database credentials in config file</li>
                <li>Contact data-team@company.com immediately</li>
            </ul>
            
            <hr/>
            <p style="font-size: 12px; color: #666;">
                This is an automated notification from the Supplier Risk & Spend Intelligence Dashboard (SRSID).
            </p>
        </body>
    </html>
    """
    return html


# ========================================
# SCHEDULED JOB
# ========================================
def scheduled_phase1_job():
    """Job to execute Phase 1 ingestion."""
    logger.info("=" * 80)
    logger.info("SCHEDULED PHASE 1 JOB STARTED")
    logger.info(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info("=" * 80)
    
    try:
        status, metrics = run_phase1_ingestion()
        
        # Send success notification
        notifier = EmailNotifier()
        subject = "✓ SRSID Phase 1: Data Ingestion Success"
        body = generate_success_email(metrics)
        
        success_emails = SCHEDULER_CONFIG.get("notifications", {}).get("on_success", {}).get("email_to", [])
        if success_emails:
            notifier.send_email(success_emails, subject, body, is_html=True)
        
        logger.info("✓ Scheduled job completed successfully")
    
    except Exception as e:
        logger.error(f"✗ Scheduled job failed: {e}")
        
        # Send failure notification
        notifier = EmailNotifier()
        subject = "✗ SRSID Phase 1: Data Ingestion FAILED"
        body = generate_failure_email(str(e))
        
        failure_emails = SCHEDULER_CONFIG.get("notifications", {}).get("on_failure", {}).get("email_to", [])
        if failure_emails:
            notifier.send_email(failure_emails, subject, body, is_html=True)


# ========================================
# SCHEDULER SETUP
# ========================================
class PipelineScheduler:
    """Manages the scheduled pipeline execution."""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.is_running = False
    
    def start(self):
        """Start the scheduler."""
        try:
            trigger = CronTrigger(
                day_of_week=SCHEDULER_CONFIG["day_of_week"],
                hour=SCHEDULER_CONFIG["hour"],
                minute=SCHEDULER_CONFIG["minute"],
                timezone=SCHEDULER_CONFIG["timezone"],
            )
            
            self.scheduler.add_job(
                scheduled_phase1_job,
                trigger=trigger,
                id="phase1_ingestion",
                name="Phase 1 Data Ingestion",
                misfire_grace_time=300,  # 5 minute grace period
                max_instances=SCHEDULER_CONFIG["max_instances"],
                replace_existing=True,
            )
            
            self.scheduler.start()
            self.is_running = True
            
            logger.info("=" * 80)
            logger.info("PIPELINE SCHEDULER STARTED")
            logger.info(f"Scheduled: Phase 1 Ingestion")
            logger.info(f"Trigger: Every {SCHEDULER_CONFIG['day_of_week']} at {SCHEDULER_CONFIG['hour']:02d}:{SCHEDULER_CONFIG['minute']:02d} {SCHEDULER_CONFIG['timezone']}")
            logger.info("=" * 80)
        
        except SchedulerAlreadyRunningError:
            logger.warning("⚠ Scheduler is already running")
        except Exception as e:
            logger.error(f"✗ Failed to start scheduler: {e}")
            raise
    
    def stop(self):
        """Stop the scheduler."""
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("✓ Scheduler stopped")
    
    def get_scheduled_jobs(self):
        """Get list of scheduled jobs."""
        return self.scheduler.get_jobs()
    
    def trigger_now(self):
        """Manually trigger Phase 1 job immediately (for testing)."""
        logger.info("⚡ Manually triggering Phase 1 job...")
        scheduled_phase1_job()


# ========================================
# MAIN
# ========================================
if __name__ == "__main__":
    # Initialize scheduler
    scheduler = PipelineScheduler()
    
    # Start scheduler
    scheduler.start()
    
    # Keep scheduler running
    try:
        logger.info("Scheduler is running. Press CTRL+C to stop.")
        while True:
            pass
    except KeyboardInterrupt:
        logger.info("Shutting down scheduler...")
        scheduler.stop()
        logger.info("Scheduler stopped.")
