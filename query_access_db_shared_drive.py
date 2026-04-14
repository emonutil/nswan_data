import os
import subprocess
import pandas as pd
import pyodbc

from sqlalchemy import create_engine
from io import StringIO
from rich.traceback import install
from dotenv import dotenv_values
from loguru import logger
from time import perf_counter
from halo import Halo


install()
config = dotenv_values()

logger.remove()
logger.add('/opt/automation/netops/nswan_data/logs/accessdb.log', rotation="5 MB")

# Provide your Windows share details here
windows_host = config['WINDOWS_HOST']
share_name = config['SHARE_NAME']
mount_point = config['MOUNT_POINT']  # Local directory to mount the share
username = config['SVC_ACCT_USER']  # Windows username
password = config['SVC_ACCT_PASS']  # Windows password

# Path to Access .mdb or .accdb file on mounted share
access_db_path = config['DB_PATH']

tables = ['Circuits', 'Hardware', 'Ports', 'Service', 'PORTSPreposed']


def send_email(message):

    recipients = ['mark.king@nc.gov', 'jerry.hampton@nc.gov']

    body = f"""
    <html>
        <body>
            <p style="color:red;">
                <strong>
                    *** Please do NOT reply to this email. ***
                </strong>
            </p>
            <p>Team,<br>
                {message}<br>
            </p>
            <br>
            <p><b>Automation w/ Python</b></p>
        </body>
    </html>
    """

    try:

        server = smtplib.SMTP("outbound.mail.nc.gov:25")
        message = MIMEMultipart()
        message['From'] = formataddr(
                (str(
                    Header('Python Script', 'utf-8')
                    ), 'eMonDevTeam@nc.gov')
                )

        message['To'] = ', '.join(recipients)
        # message['Bcc'] = ','.join(bcc_recipients)
        message['Subject'] = "Issue w/ Copying NSWAN Data from MS Access"
        message.attach(MIMEText(body, "html"))

        # for attachment in attachments:
            # with open(attachment, 'rb') as f:
                # # Attach the file with filename to the email
                # message.attach(
                        # MIMEApplication(
                            # f.read(),
                            # Name=os.path.basename(attachment)
                            # )
                        # )

        server.sendmail(
                message['From'],
                recipients, # + bcc_recipients,
                message.as_string()
                )
        logging.info('Email sent!')
        server.quit()
        result = True

    except Exception as e:
        logger.error(f'Failed to send email: {e}')
        result = False

    return result


def query_access_table(db_path, table):
    try:
        cmd = ["mdb-export", db_path, table]
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False)  # get bytes, not str

        if result.returncode != 0:
            err = result.stderr.decode('cp1252', errors='replace')
            raise Exception(err)

        output = result.stdout.decode('cp1252', errors='replace')
        df = pd.read_csv(StringIO(output), low_memory=False)
        return df

    except Exception as e:
        logger.error(f"Query failed: {e}")
        return None


def mount_windows_share(
        share_name, mount_point, username, password, windows_host):
    """
    Mount a Windows network share to a Linux directory using hostname.

    :param share_name: The name of the shared folder on the Windows machine.
    :param mount_point: Directory where you want to mount the network share.
    :param username: The Windows username.
    :param password: The Windows password.
    :param windows_host: The Windows machine hostname or NetBIOS name .
    """
    # Ensure the mount point directory exists
    if not os.path.exists(mount_point):
        os.makedirs(mount_point)

    # Command to mount the Windows network share
    mount_command = [
        "sudo", "mount", "-t", "cifs",
        f'//{windows_host}/{share_name}', mount_point,
        "-o", f"username={username},password={password},vers=3.0"
    ]

    try:
        subprocess.run(mount_command, check=True)
        logger.info(
            f"Mounted //{windows_host}/{share_name} to {mount_point}"
            " successfully.")

        for table in tables:

            # Query Access db
            df = query_access_table(access_db_path, table)

            # Set up PostgreSQL connection
            engine = create_engine(
                f"postgresql+psycopg2://{config['PG_USER']}:{config['PG_PASS']}@{config['PG_HOST']}:{config['PG_PORT']}/{config['PG_DB_NAME']}")

            # Upload to PostgreSQL
            if df is not None:
                df.to_sql(table, engine, if_exists="replace", index=False)
                logger.info("Data inserted successfully.")
            else:
                logger.info("No data to insert — query failed.")

        # Unmount the share
        logger.debug("Unmounting network share...")
        try:
            subprocess.run(['sudo', 'umount', mount_point], check=True)
            logger.info("Unmounted successfully.")
        except subprocess.CalledProcessError as e:
            message = f"Failed to unmount: {e}"
            logger.error(message)
            try:
                send_email(message)
            except Exception as e:
                logger.error(f'Failed to send email after failed to unmount: {e}')
    except subprocess.CalledProcessError as e:
        message = f'Failed to mount the network share. A subprocess.CalledProcessError: {e}'
        try:
            send_email(message)
        except Exception as e:
            logger.error(f'Failed to send email after failed mount: {e}')
        logger.error(message)


def main():

    # spinner = Halo(
    #     text='Copying data from NSWAN to PostgreSQL...', spinner='dots')
    # spinner.start()
    start = perf_counter()
    # Mount the share
    mount_windows_share(
        share_name, mount_point, username, password, windows_host)
    end = perf_counter()
    logger.debug(f'Finished script in {(end - start):.2f} second(s).')
    # spinner.succeed(f'Completed in {(end - start):.2f} second(s).')


if __name__ == '__main__':
    main()
