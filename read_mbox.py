from __future__ import print_function

from concurrent.futures import wait, ThreadPoolExecutor
import json
import logging
import mailbox
import os
import pickle
import time
from io import BytesIO

from apiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError, MediaUploadSizeError
from tqdm import tqdm, trange, tnrange

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SCOPES = ['https://www.googleapis.com/auth/apps.groups.migration']


def process_list(service, data, alias):
    mbox_files = data[alias]

    file_num = 0
    for mbox_path in mbox_files:
        try:
            mb = mailbox.mbox(mbox_path)
        except e:
            logger.error('Could not read mbox', e)

        logger.info(f'Starting with mbox: {mbox_path}')

        for i in trange(len(mb) - 100, desc=f'{alias} {file_num}'):
            i += 100
            msg = mb[i]
            start_time = time.time()

            try:
                media = MediaIoBaseUpload(BytesIO(msg.as_bytes()),
                                          mimetype='message/rfc822')
                insert_request = service.archive().insert(
                    groupId=alias, media_body=media,
                    media_mime_type='message/rfc822'
                )
                insert_request.execute()
            except (HttpError, MediaUploadSizeError) as e:
                logger.warning(
                    f'Message {msg.get("Message-ID")} failed for archive {alias} ({type(e)})')
                with open(f'errors/{alias}-{i}.eml', 'w') as error_file:
                    error_file.write(msg.as_string())

            speed_time = time.time() - start_time

            if speed_time < 1:
                time.sleep(1 - speed_time)

        file_num += 1


def main():
    try:
        os.mkdir('./errors')
    except:
        pass  # ignore

    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('groupsmigration', 'v1', credentials=creds)

    with open('config.json') as json_file:
        data = json.load(json_file)

        for alias in data.keys():
            process_list(service, data, alias)


if __name__ == '__main__':
    main()
