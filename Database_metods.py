import threading
import sqlite3
import time
from typing import List, Dict
from loguru import logger

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


class DB:

    def __init__(self):
        self.conn = sqlite3.connect('Database_resources/database.db')
        self.cur = self.conn.cursor()
        self.lock = threading.Lock()

        self.cur.execute("""
      CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task TEXT,
        user_id TEXT,
        can_send INTEGER,
        time INTEGER,
        answer_time INTEGER,
        was_sent INTEGER,
        is_done INTEGER,
        ignore_this_task INTEGER
      )
    """)

    @logger.catch
    def task_exists(self, task_text: str):
        self.cur.execute("SELECT EXISTS(SELECT 1 FROM tasks WHERE task = ?)", (task_text,))
        return bool(self.cur.fetchone()[0])

    @logger.catch
    def insert_data(self, tasks: List[Dict]):
        with self.lock:
            for task_dct in tasks:
                # logger.debug(f'{task_dct=}')
                # logger.debug(f'{task_dct["task"]=}')
                # logger.debug(f'{self.task_exists(task_dct["task"])=}')
                if not self.task_exists(task_dct['task']):
                    # logger.debug(f'{task_dct=}')
                    self.cur.execute("""
            INSERT INTO tasks (task, user_id, can_send, time, answer_time, was_sent, is_done, ignore_this_task)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          """, (
                        task_dct['task'],
                        task_dct['user_id'],
                        task_dct['can_send'],
                        time.time(),
                        time.time() + int(task_dct['answer_time'])*60,
                        0,
                        0,
                        0
                    )
                                     )
            self.conn.commit()

    @logger.catch
    def get_tasks(self) -> List[Dict]:
        with self.lock:
            self.cur.execute("SELECT * FROM tasks WHERE was_sent = 0")
            rows = self.cur.fetchall()
            result = []
            # logger.debug(f'{rows=}')
            for row in rows:
                result.append({
                    'id': row[0],
                    'task': row[1],
                    'user_id': row[2],
                    'can_send': bool(row[3]),
                    'was_sent': bool(row[4]),
                    'time_for_task': round((row[5] - row[4])/60)
                })
            # logger.debug(f'{result=}')
            return result

    @logger.catch
    def mark_task_sent(self, task_text: str):
        with self.lock:
            self.cur.execute("UPDATE tasks SET was_sent = 1 WHERE task = ?", (task_text,))
            self.conn.commit()

    @logger.catch
    def mark_is_done(self, task_text: str, res: int):
        with self.lock:
            self.cur.execute(f"UPDATE tasks SET is_done = {res} WHERE task = ?", (task_text,))
            self.conn.commit()

    @logger.catch
    def get_task_by_id(self, task_id: int) -> str:
        with self.lock:
            self.cur.execute("SELECT * FROM tasks WHERE id=?", (task_id,))
            row = self.cur.fetchone()
            if row:
                return row[1]

    @logger.catch
    def get_task_id_by_text(self, task_text: str) -> int:
        with self.lock:
            self.cur.execute("SELECT id FROM tasks WHERE task=?", (task_text,))
            row = self.cur.fetchone()
            if row:
                return row[0]
            else:
                return None

    @logger.catch
    def get_ignored_tasks(self) -> List[Dict]:
        """

        :return: [{'id': int,
                 'task': str,
                 'user_id': str,
                 'remaining_time': int
                 }]
        """
        with self.lock:
            self.cur.execute("SELECT * FROM tasks WHERE is_done = 0")
            rows = self.cur.fetchall()
            result = []
            # logger.debug(f'{rows=}')
            for row in rows:
                result.append({
                    'id': row[0],
                    'task': row[1],
                    'user_id': row[2],
                    'remaining_time': int(row[5]) - int(time.time())
                })
            # logger.debug(f'{result=}')
            return result
