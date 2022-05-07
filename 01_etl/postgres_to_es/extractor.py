from contextlib import contextmanager
from datetime import datetime

import psycopg2
from dotenv import load_dotenv
from psycopg2 import OperationalError
from psycopg2.extras import DictCursor

import pg_sql
from backoff import backoff

load_dotenv()

MESSAGE = "DB connection error."


@contextmanager
def pg_conn_context(dsl: dict):
    conn = None
    message = MESSAGE

    @backoff(OperationalError, message)
    def connect(dsl):
        conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
        conn.cursor()
        return conn

    conn = connect(dsl)
    yield conn

    conn.close()


class PgExtractor:
    def __init__(self, connection) -> None:
        self.conn = connection
        self.cursor = self.conn.cursor()

    @backoff(OperationalError, MESSAGE)
    def get_last_modified(self) -> datetime:
        """
        Метод для получения наибольшей даты обновления всех объектов
        Возвращает максимальное значение поля modified из таблиц:
        - person
        - genre
        - film_work
        """
        self.cursor.execute(pg_sql.PG_LAST_MODIFIED)
        return dict(self.cursor.fetchone()).get("modified")

    @backoff(OperationalError, MESSAGE)
    def get_movies_to_update(self, last_modified: datetime) -> list:
        """Метод для получения списка идентификаторов кинопроизведений для обновления.

        :param last_modified: дата, используется для фильтрации выборки.

        Возвращает кинопроизведения, в которых дата одновления одного или нескольких
        из объектов person, genre, film_work - больше параметра last_modified

        При наличии обновлений в нескольких объектах кинопроизведения (например, в связанных genre и person)
        для каждого кинопроизведения возвращается максимальная дата обновления.
        """
        self.cursor.execute(pg_sql.PG_MOVIES_TO_UPDATE, {"date": last_modified})
        data = [dict(row) for row in self.cursor.fetchall()]
        return data

    @backoff(OperationalError, MESSAGE)
    def select_all_movies(self) -> None:
        """ "
        Метод для выбора всех кинопроизведений в курсор
        """
        self.cursor.execute(pg_sql.PG_SELECT_ALL)

    @backoff(OperationalError, MESSAGE)
    def select_movies(self, ids: tuple) -> None:
        """
        Метод для выбора кинопроизведений по списку в курсор

        :param ids: кортеж идентификаторов кинопроизведений
        """
        self.cursor.execute(pg_sql.PG_SELECT_BY_ID, (ids,))

    @backoff(OperationalError, MESSAGE)
    def extract_batch(self, batch_size=100) -> list:
        """
        Метод для получения пакета записей из курсора

        :param batch_size: размер пакета. по умолчанию равен 100 записям

        Возвращает список кинопроизведений в формате,
        пригодном для загрузки в индекс Elasticsearch
        """
        batch_data = [dict(row) for row in self.cursor.fetchmany(batch_size)]
        return batch_data
