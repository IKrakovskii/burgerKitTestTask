import asyncio
import os.path
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build
from loguru import logger
from TG_bot import send_message_from_user, send_message_from_admin
from Database_metods import DB

logger.add(
    'logs/debug.log',
    format='{time} {level} {message}',
    level='DEBUG'
)
logger.add(
    'logs/errors.log',
    format='{time} {level} {message}',
    level='WARNING'
)

logger.info('Ссылка на таблицу: https://clck.ru/352NGH')


class GoogleSheet:
    def __init__(self):
        self.get_rows = 200
        self.SAMPLE_SPREADSHEET_ID = '10OO5vYzIV-HcuBuuflRgd966NuNC0GZijr0lHUe-oT8'
        self.SAMPLE_RANGE_NAME = f'tasks!A1:F{self.get_rows}'
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        self.spreadsheet_id = \
            'https://docs.google.com/spreadsheets/d/10OO5vYzIV-HcuBuuflRgd966NuNC0GZijr0lHUe-oT8/edit#gid=0'
        self.credentials_path = 'Tokens_and_passwords/credentials.json'
        # self.db_path = 'Database_resources/database.db'
        self.table_name = 'tasks'

    @logger.catch
    def get_data_from_google_table(self) -> dict[str] | None:
        out_dct = {
            'task': [],
            'time': [],
            'answer_time': [],
            'user_id': [],
            'can_send': [],
            'was_sent': []
        }
        creds = None
        if os.path.exists('Tokens_and_passwords/token.json'):
            creds = Credentials.from_authorized_user_file('Tokens_and_passwords/token.json', self.SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'Tokens_and_passwords/credentials.json', self.SCOPES)
                creds = flow.run_local_server(port=0)
            with open('Tokens_and_passwords/token.json', 'w') as API_token:
                API_token.write(creds.to_json())

        try:
            service = build('sheets', 'v4', credentials=creds)

            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=self.SAMPLE_SPREADSHEET_ID,
                                        range=self.SAMPLE_RANGE_NAME).execute()
            values = result.get('values', [])
            # logger.debug(f'{values=}')
            if not values:
                logger.error('ОШИБКА СЕРВЕРА, НЕТ ДАННЫХ ТАБЛИЦЫ')
                return
            # logger.debug(f'{values[1:]=}')
            for row in values[1:]:
                if not row or len(row) < 5:
                    break
                out_dct['task'].append(row[0])
                out_dct['time'].append(row[1])
                out_dct['answer_time'].append(row[2])
                out_dct['user_id'].append(row[3])
                out_dct['can_send'].append(True)
                out_dct['was_sent'].append(0)
            # logger.debug(f'{out_dct=}')
            return out_dct
        except HttpError as err:
            logger.error(f'HTTP Error: {err}')


@logger.catch
async def parsing_tasks():
    db = DB()
    while True:
        get_info_from_google_table = GoogleSheet()
        task_dct = get_info_from_google_table.get_data_from_google_table()
        # logger.debug(f'{task_dct=}')
        for i in range(len(task_dct['task'])):
            # logger.info(f'{task_dct=}')
            task, user_id, can_send, was_sent, answer_time = \
                task_dct['task'][i], \
                    task_dct['user_id'][i], \
                    task_dct['can_send'][i], \
                    task_dct['was_sent'][i], \
                    int(task_dct['answer_time'][i])
            db.insert_data([
                {'task': task,
                 'user_id': user_id,
                 'can_send': can_send,
                 'was_sent': was_sent,
                 'answer_time': answer_time
                 }])
            # logger.debug(f'\n\t{db.task_exists(task_text=task)=}\n\t{task=}')
            tasks_for_bot_send = db.get_tasks()
            # logger.debug(f'{tasks_for_bot_send=}')
            # logger.info(f'{db.get_tasks()=}')

        for i in range(len(tasks_for_bot_send)):
            r = await send_message_from_user(
                task_text=tasks_for_bot_send[i]['task'],
                user_id=tasks_for_bot_send[i]['user_id'],
                time_for_task=tasks_for_bot_send[i]['time_for_task']
            )
            if r:
                logger.info(
                    f'Сообщение {tasks_for_bot_send[i]["task"]} дошло до пользователя '
                    f'{tasks_for_bot_send[i]["user_id"]} успешно'
                )
                db.mark_task_sent(task_text=tasks_for_bot_send[i]['task'])

        ignored_task = db.get_ignored_tasks()

        for i in range(len(ignored_task)):
            logger.debug(f'{ignored_task=}')
            if ignored_task[i]['remaining_time'] < 0:
                r = await send_message_from_admin(
                    text=ignored_task[i]['task'],
                    id=ignored_task[i]['user_id'],
                    result='ignore',
                    username=None

                )
                if r:
                    logger.info(
                        f'Сообщение\nПользователь c id: '
                        f'{ignored_task[i]["user_id"]} '
                        f'Проигнорировал задание: {ignored_task[i]["task"]} '
                        f'дошло до пользователя '
                        f'{ignored_task[i]["user_id"]} успешно'
                    )
                    db.mark_task_sent(task_text=ignored_task[i]['task'])
                    db.mark_is_done(task_text=ignored_task[i]['task'], res=3)

        await asyncio.sleep(10)
