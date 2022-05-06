from elasticsearch import Elasticsearch
from elasticsearch import ConnectionError
from elasticsearch import helpers
from contextlib import contextmanager
from psycopg2.extensions import connection as _connection
from logger import log
import json
import os
from dotenv import load_dotenv
from backoff import backoff
from datetime import datetime
import extractor
import state
import time


@contextmanager
def es_conn_context(url: str):
    """Контекстный менеджер для подключения к Elasticsearch"""
    es = None
    message = "Возникла ошибка подключения к Elasticsearch."

    @backoff(ConnectionError, message)
    def connect(url):
        es = Elasticsearch(url)
        log.info(es.info())
        return es

    es = connect(url)
    yield es


def load_to_es(es_client: Elasticsearch, pg_connection: _connection) -> None:
    """Основной метод для инкрементальной миграции данных из БД Postgres в индекс movies в Elasticsearch"""
    INDEX_NAME = "movies"
    STATE_STORAGE = "state_storage.json"
    INDEX_SCHEMA = "es_schema.json"
    BATCH_SIZE = 100

    pg_extractor = extractor.PgExtractor(pg_connection)

    # Создание индекса , если уже не существует индекса с таким именем
    if not es_client.indices.exists(index=INDEX_NAME):
        with open(INDEX_SCHEMA) as f:
            schema = json.loads(f.read())
            es_client.indices.create(index=INDEX_NAME, **schema)
            log.info("Создан индекс {}".format(INDEX_NAME))

    def load_batches():
        """
        Функция для получения пакета данных из Postgres и пакетной загрузки в Elasticsearch
        """
        while True:
            log.info("Extracting batch of {}".format(BATCH_SIZE))
            data = pg_extractor.extract_batch(BATCH_SIZE)
            if not data:
                break

            def gendata():
                """
                Получение генератора json для использования в пакетной загрузке в Elasticsearch
                """
                for row in data:
                    yield {"_index": INDEX_NAME, "_id": row["id"], "_source": row}

            result = helpers.bulk(es_client, gendata(), stats_only=True)
            log.debug(result)
            log.info("Batch loaded")

    # Получаем последнее состояние из файла
    file_storage = state.JsonFileStorage(STATE_STORAGE)
    curr_state = state.State(file_storage)
    last_modified = curr_state.get_state("modified")

    # Если файл пуст, выгружаем все фильмы в индекс
    if last_modified == None:
        log.info("Last modified = {}".format(last_modified))
        pg_extractor.select_all_movies()
        load_batches()
        last_modified = str(pg_extractor.get_last_modified())
        log.info("last_modified = {}".format(last_modified))
    # Если в файле есть состояние, загружаем фильмы, обновленные позже даты из файла
    else:
        log.info("Last modified = {}".format(last_modified))
        movies = pg_extractor.get_movies_to_update(
            datetime.fromisoformat(last_modified)
        )
        if movies:
            ids = tuple(row["id"] for row in movies)
            pg_extractor.select_movies(ids)
            load_batches()
            last_modified = str(max(row["modified"] for row in movies))
            log.info("last_modified = {}".format(last_modified))

    # Устанавливаем новое значения состояния
    curr_state.set_state("modified", last_modified)


if __name__ == "__main__":
    load_dotenv()
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }
    while True:
        with es_conn_context(os.environ.get("ES_URL")) as es, extractor.pg_conn_context(
            dsl
        ) as pg:
            load_to_es(es, pg)
            log.info(
                "Следующая синхронизация через {} секунд.".format(
                    os.environ.get("LOOP_TIMEOUT")
                )
            )
        time.sleep(int(os.environ.get("LOOP_TIMEOUT")))
