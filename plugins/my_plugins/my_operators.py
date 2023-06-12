from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import requests
import imaplib
import email
import re,csv
from airflow import DAG
from email.parser import BytesParser
from airflow.utils.dates import days_ago
from datetime import datetime

import gspread
from email.utils import parseaddr, parsedate_to_datetime
from oauth2client.service_account import ServiceAccountCredentials

import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# def update_conection(connection_id):
#             url = f"http://127.0.0.1:5000/api/update-connections/{connection_id}"
#             response = requests.get(url)
#             if response.status_code == 200:
#                 connection = response.json()
#                 return connection
#             else:
#                 return None

def get_connection(connection_id):
            url = f"http://127.0.0.1:5000/api/get-connections/{connection_id}"
            response = requests.get(url)
            if response.status_code == 200:
                connection = response.json()
                return connection
            else:
                return None

connection_id = "connectionfor_imapp" 
connection = get_connection(connection_id)
if connection is None:
    raise ValueError(f"Connection with ID '{connection_id}' not found.")

imap_url = connection['host']
username = connection['login']
password = connection['password']

class IMAPPluginOperator(BaseOperator):
    @apply_defaults
    def __init__(self ,*args, **kwargs):
        super().__init__(*args, **kwargs)
        self.imap_server = imap_url
        self.imap_port = 993
        self.imap_username = username
        self.imap_password = password

        
    def execute(self, context):
        print(self.imap_server)
        print(self.imap_port)
        print(self.imap_username)
        print(self.imap_password)

        self.xcom_push(context=context, key='imap_server', value=self.imap_server)
        self.xcom_push(context=context, key='imap_port', value=self.imap_port)
        self.xcom_push(context=context, key='imap_username', value=self.imap_username)
        self.xcom_push(context=context, key='imap_password', value=self.imap_password)

        def extract_email_body(email_message):
            email_body = ''
            if email_message.is_multipart():
                for part in email_message.walk():
                    content_type = part.get_content_type()
                    if content_type == 'text/plain':
                        try:
                            email_body = part.get_payload(decode=True).decode("utf-8")
                        except UnicodeDecodeError:
                            email_body = part.get_payload(decode=True).decode("latin-1", errors='replace')
                        break
            else:
                content_type = email_message.get_content_type()
                if content_type == 'text/plain':
                    try:
                        email_body = email_message.get_payload(decode=True).decode("utf-8")
                    except UnicodeDecodeError:
                        email_body = email_message.get_payload(decode=True).decode("latin-1", errors='replace')
            return email_body
        
        try:
            # Connect to the IMAP server
            mail = imaplib.IMAP4_SSL(self.imap_server, self.imap_port)
            mail.login(self.imap_username, self.imap_password)
            mail.select('INBOX')
            result, data = mail.search(None, 'ALL')
            sare = []
            if result == 'OK':
                email_ids = data[0].split()
                for email_id in email_ids:
                    result2, email_data = mail.fetch(email_id, '(RFC822)')
                    if result2 == 'OK':
                        raw_email = email_data[0][1]
                        email_message = email.message_from_bytes(raw_email)
                        email_body = extract_email_body(email_message)
                        to_address = email_message['To']
                        from_address = email.utils.parseaddr(email_message['From'])[1]
                        subject = email_message['Subject']
                        timestamp = email_message['Date']
                        imestamp = email_message['Date']
                        tz_info = re.search(r'\((.*?)\)', timestamp)
                        timestamp = re.sub(r'\(.*?\)', '', timestamp).strip()

                        print(f'timestamp from mail: {timestamp}')

                        try:
                            timestamp_datetime = datetime.strptime(timestamp, "%a, %d %b %Y %H:%M:%S %z")
                        except:
                            timestamp_datetime = datetime.strptime(timestamp, "%a, %d %b %Y %H:%M:%S GMT")
                        date_without_timezone = timestamp_datetime.replace(tzinfo=None)

                        # timestamp = re.sub(r'\(.*?\)', '', timestamp).strip()
                    sare.append({
                    "From_Address": from_address,
                    "To_Address": to_address,
                    "Subject": subject,
                    "Time": str(date_without_timezone),
                    "Content": email_body
                })
                    
            csv_file_path = "inbox.csv"
            if len(sare)==0:
                print("no emails in inbox")
            else:
                fieldnames = sare[0].keys()
                with open(csv_file_path, "w", newline="") as file:
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(sare)
                print("Data received to CSV file from INBOX")
        except Exception as e:
            print(f'Error fetching inbox mails: {str(e)}')
            raise e
        self.xcom_push(context=context, key='csv_file_path', value=csv_file_path)
        mail.close()
        mail.logout()

class SpreadsheetPluginOperator(BaseOperator):
    @apply_defaults
    def __init__(self, keys, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.keys = keys
# Jun 9, 2023, 4:09 PM
# Jun 9, 2023, 4:15 PM
        
    def execute(self, context):

        imap_server = context['ti'].xcom_pull(task_ids='email_login', key='imap_server')
        imap_port = context['ti'].xcom_pull(task_ids='email_login', key='imap_port')
        imap_username = context['ti'].xcom_pull(task_ids='email_login', key='imap_username')
        imap_password = context['ti'].xcom_pull(task_ids='email_login', key='imap_password')
        csv_file_path = context['ti'].xcom_pull(task_ids='email_login', key='csv_file_path')

        print(self.keys)

        self.xcom_push(context=context, key='keys', value=self.keys)
        def spreadsheet(keys,csvfile):
            secret_key = keys
            credentials = ServiceAccountCredentials.from_json_keyfile_name(secret_key)
            client = gspread.authorize(credentials)
            # spreadsheet_name = 'My Spreadsheett'
            spreadsheet_name=imap_username
            try:
                spreadsheet = client.open(spreadsheet_name)
                print(f"The spreadsheet '{spreadsheet_name}' exists.")
                role = 'writer'  
                # with open(csvfile, 'r') as file:
                #     csv_reader = csv.reader(file)
                #     next(csv_reader, None)
                #     for _ in range(1 - 1):
                #         next(csv_reader, None)
                #     for row in csv_reader:
                #         values =  row[2 - 1]
                # spreadsheet.share(imap_username, perm_type='user', role=role)
                sheet = spreadsheet.get_worksheet(0)
                def count_rows(csv_file):
                    with open(csv_file, 'r') as file:
                        reader = csv.reader(file)
                        row_count = sum(1 for row in reader)
                    return row_count
                num_rows = count_rows(csvfile)
                csv_file_path = csvfile
                csv_data = []
                with open(csv_file_path, 'r') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        csv_data.append(row)
                print(num_rows)
                sheetdata = sheet.get_all_values()
                if len(sheetdata)==num_rows:
                    print("no new info fetched")
                else:
                    difference = [x for x in csv_data if x not in sheetdata]
                    for row in difference:
                        sheet.append_row(row)
                    print("Information from mail added to spread sheet")
                spreadsheet = client.open(spreadsheet_name)
                spreadsheet_url = spreadsheet.url
                print(f"The URL of the spreadsheet '{spreadsheet_name}' is: {spreadsheet_url}")

            except gspread.SpreadsheetNotFound:
                spreadsheet = client.create(spreadsheet_name)
                print(f"The spreadsheet '{spreadsheet_name}' created.")
                role = 'writer'  
                # with open(csvfile, 'r') as file:
                #     csv_reader = csv.reader(file)
                #     next(csv_reader, None)
                #     for _ in range(1 - 1):
                #         next(csv_reader, None)
                #     for row in csv_reader:
                #         values =  row[2 - 1]
                spreadsheet.share(imap_username, perm_type='user', role=role)
                sheet = spreadsheet.get_worksheet(0)
                def count_rows(csv_file):
                    with open(csv_file, 'r') as file:
                        reader = csv.reader(file)
                        row_count = sum(1 for row in reader)
                    return row_count
                num_rows = count_rows(csvfile)
                csv_file_path = csvfile
                csv_data = []
                with open(csv_file_path, 'r') as file:
                    csv_reader = csv.reader(file)
                    for row in csv_reader:
                        csv_data.append(row)
                print(num_rows)
                sheetdata = sheet.get_all_values()
                if len(sheetdata)==num_rows:
                    print("no new info fetched")
                else:
                    difference = [x for x in csv_data if x not in sheetdata]
                    for row in difference:
                        sheet.append_row(row)
                    print("Information from mail added to spread sheet")
                    spreadsheet = client.open(spreadsheet_name)
                    spreadsheet_url = spreadsheet.url
                    print(f"The URL of the spreadsheet '{spreadsheet_name}' is: {spreadsheet_url}")
        spreadsheet(self.keys,csv_file_path)

class DrivePluginOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        
    def execute(self, context):
        def process_email_attachments():

            imap_server = context['ti'].xcom_pull(task_ids='email_login', key='imap_server')
            imap_port = context['ti'].xcom_pull(task_ids='email_login', key='imap_port')
            imap_username = context['ti'].xcom_pull(task_ids='email_login', key='imap_username')
            imap_password = context['ti'].xcom_pull(task_ids='email_login', key='imap_password')
            keys = context['ti'].xcom_pull(task_ids='spreadsheet_task', key='keys')




            mail = imaplib.IMAP4_SSL(imap_server)
            mail.login(imap_username, imap_password)
            mail.select('INBOX')
            credentials_file = keys
            SCOPES = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']
            result, data = mail.search(None, 'ALL')
            def upload_image_to_drive(image_path):
                credentials = service_account.Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
                drive_service = build('drive', 'v3', credentials=credentials)
                file_metadata = {'name': os.path.basename(image_path)}
                media = MediaFileUpload(image_path, mimetype='image/jpeg')
                file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                file_id = file.get('id')
                return file_id
            def open_image_by_name(image_filename):
                return os.path.join('attachments', image_filename)
            def share_image_with_others(file_id, email_addresses):
                credentials = service_account.Credentials.from_service_account_file(credentials_file, scopes=SCOPES)
                drive_service = build('drive', 'v3', credentials=credentials)
                batch_permission = {'role': 'reader', 'type': 'user', 'emailAddress': email_addresses}
                drive_service.permissions().create(fileId=file_id, body=batch_permission).execute()
            attachments = []
            try:
                if result == 'OK':
                    email_ids = data[0].split()
                    for email_id in email_ids:
                        result2, email_data = mail.fetch(email_id, '(RFC822)')
                        if result2 == 'OK':
                            raw_email = email_data[0][1]
                            email_message = email.message_from_bytes(raw_email)
                            from_address = email.utils.parseaddr(email_message['To'])[1]
                            timestamp = email_message['Date']
                            timestamp = re.sub(r'\(.*?\)', '', timestamp).strip()
                            # timestamp_datetime = datetimestrptime(timestamp, "%a, %d %b %Y %H:%M:%S %z")
                            
                            print(f'timestamp from mail: {timestamp}')

                            try:
                                timestamp_datetime = datetime.strptime(timestamp, "%a, %d %b %Y %H:%M:%S %z")
                            except:
                                timestamp_datetime = datetime.strptime(timestamp, "%a, %d %b %Y %H:%M:%S GMT")

                            date_without_timezone = timestamp_datetime.replace(tzinfo=None)
                            save_directory = 'attachments'
                            if not os.path.exists(save_directory):
                                os.makedirs(save_directory)
                            for part in email_message.walk():
                                if part.get_content_maintype() == 'multipart':
                                    continue
                                if part.get('Content-Disposition') is None:
                                    continue
                                filename = part.get_filename()
                                if filename:
                                    filepath = os.path.join(save_directory, filename)
                                    with open(filepath, 'wb') as f:
                                        f.write(part.get_payload(decode=True))
                                    print('Downloaded attachment:', filename)
                                    attachments.append({
                                        "Image_Name": filename,
                                        "To": from_address,
                                        "Time": str(date_without_timezone),
                                        "sent": None
                                    })
                csv_file_path = 'attachment_details.csv'
                if attachments:
                    fieldnames = attachments[0].keys()
                    with open(csv_file_path, "w", newline="") as file:
                        writer = csv.DictWriter(file, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(attachments)
                    print("Attachment details saved to CSV file.")
                else:      
                    print("No attachments found in emails.")
            except Exception as e:
                print(f'Error fetching inbox mails: {str(e)}')
                raise e
            mail.close()
            mail.logout()
            # with open('attachment_details.csv', 'r') as file:
            #     csv_reader = csv.reader(file)
            #     next(csv_reader, None)
            #     for _ in range(1 - 1):
            #         next(csv_reader, None)
            #     for row in csv_reader:
            #         values =  row[2 - 1]
            secret_key = keys
            credentials = ServiceAccountCredentials.from_json_keyfile_name(secret_key)
            client = gspread.authorize(credentials)
            spreadsheet_name = imap_username+imap_password
            def count_rows(csv_file):
                with open(csv_file, 'r') as file:
                    reader = csv.reader(file)
                    row_count = sum(1 for row in reader)
                return row_count
            num_rows = count_rows('attachment_details.csv')
            email_address = email_message['To']
            csv_file_path = 'attachment_details.csv'
            csv_data = []
            with open(csv_file_path, 'r') as file:
                csv_reader = csv.reader(file)
                for row in csv_reader:
                    csv_data.append(row)
            def send_attachments_to_drive(attaching):
                send_image=open_image_by_name(attaching)
                recipients = imap_username
                file_id = upload_image_to_drive(send_image)
                share_image_with_others(file_id, recipients.strip())
                print(f"Attachment '{os.path.basename(send_image)}' sent successfully to Google Drive of recipients")
                print("All attachments sent successfully to Google Drive!")
            try:
                spreadsheet = client.open(spreadsheet_name)
                print(f"The spreadsheet '{spreadsheet_name}' exists.")
                sheet = spreadsheet.get_worksheet(0)
                sheetdata = sheet.get_all_values()
                if len(sheetdata)==num_rows:
                    print("no new info fetched")
                else:
                    difference =[x for x in csv_data if x[:-1] not in [row[:-1] for row in sheetdata]]
                    for row in difference:
                        sheet.append_row(row)
                    print("Information from mail added to spread sheet")
                sheetdata = sheet.get_all_values()
                for i, row in enumerate(sheetdata[1:], start=2):
                    if row[-1] == '':
                        images = row[-4]
                        send_attachments_to_drive(images)
                        sheet.update_cell(i, len(row), 'sent')
                        print("sent")
            except gspread.SpreadsheetNotFound:
                spreadsheet = client.create(spreadsheet_name)
                print(f"The spreadsheet '{spreadsheet_name}' created.") 
                spreadsheet.share(email_address, perm_type='user', role='writer')
                sheet = spreadsheet.get_worksheet(0)
                sheetdata = sheet.get_all_values()
                if len(sheetdata)==num_rows:
                    print("no new info fetched")
                else:
                    difference =[x for x in csv_data if x[:-1] not in [row[:-1] for row in sheetdata]]
                    for row in difference:
                        sheet.append_row(row)
                    print("Information from mail added to spread sheet")
                sheetdata = sheet.get_all_values()
                for i, row in enumerate(sheetdata[1:], start=2):
                    if row[-1] == '':
                        images = row[-4]
                        send_attachments_to_drive(images)
                        sheet.update_cell(i, len(row), 'sent')
                        print("sent")
            spreadsheet = client.open(spreadsheet_name)
            spreadsheet_url = spreadsheet.url
            print(f"The URL of the spreadsheet '{spreadsheet_name}' is: {spreadsheet_url}")

        process_email_attachments()

                