from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import OperationalError
from psycopg2 import InterfaceError
from contextlib import contextmanager
from dotenv import load_dotenv
from backoff import backoff
import pg_sql


load_dotenv()

MESSAGE = "Возникла ошибка подключения к БД."


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


# if __name__ == "__main__":
#     dsl = {
#         "dbname": os.environ.get("DB_NAME"),
#         "user": os.environ.get("DB_USER"),
#         "password": os.environ.get("DB_PASSWORD"),
#         "host": os.environ.get("DB_HOST"),
#         "port": os.environ.get("DB_PORT"),
#     }

#     # sql = "select * from content.film_work limit 10"
#     with pg_conn_context(dsl) as pg:
#         extractor = PgExtractor(pg)
#         last_modified = datetime.fromisoformat("2022-05-05 09:30:18.908524+00:00")
#         # latest_modified = extractor.get_latest_modified()
#         log.debug(last_modified)
#         log.debug(type(last_modified))
#         to_update = extractor.get_movies_to_update(last_modified)
#         to_update_tuple = tuple(row["id"] for row in to_update)
#         log.debug(to_update_tuple)
#         log.debug(type(to_update_tuple))
#         extractor.select_movies(to_update_tuple)
#         while True:
#             data = extractor.extract_batch()
#             if not data:
#                 break
#             for row in data:
#                 log.debug(row)

#         extractor.select_data(
#             pg_sql.PG_SELECT_ALL.format(
#                 md=datetime.datetime(
#                     2022, 5, 5, 9, 30, 17, 907877, tzinfo=datetime.timezone.utc
#                 )
#             )
#         )
#         data = extractor.extract_batch(5)
#         log.info(type(data[0]))
#         log.info(data)

#         def prepare_for_es(args: list) -> list:
#             for row in args:
#                 cur_state = row.pop("modified")
#                 log.info(cur_state)

#         prepare_for_es(data)
#         log.info(data)

# cursor = pg.cursor()
# cursor.execute(sql)
# data = [dict(row) for row in cursor.fetchall()]
