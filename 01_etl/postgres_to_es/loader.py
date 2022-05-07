import json
import os
import time
from contextlib import contextmanager
from datetime import datetime
from tkinter.messagebox import NO
from xmlrpc.client import Boolean

from dotenv import load_dotenv
from elasticsearch import ConnectionError, Elasticsearch, helpers
from psycopg2.extensions import connection as _connection

import extractor
import state
from backoff import backoff
from logger import log


@contextmanager
def es_conn_context(url: str):
    """Контекстный менеджер для подключения к Elasticsearch"""
    es = None
    message = "Elasticsearch connection error."

    @backoff(ConnectionError, message)
    def connect(url):
        es = Elasticsearch(url)
        log.info(es.info())
        return es

    es = connect(url)
    yield es


# Для ревьюера: в рамках задачи по декомпозиции функции load_to_es, вынесены в отдельные функции: load_batches и create_es_index
def load_batches(
    es_client: Elasticsearch,
    pg_extractor: extractor.PgExtractor,
    batch_size: int,
    index_name: str,
) -> None:
    """
    Функция для получения пакета данных из Postgres и пакетной загрузки в Elasticsearch
    """
    while True:
        log.info("Extracting batch of {}".format(batch_size))
        data = pg_extractor.extract_batch(batch_size)
        if not data:
            break

        def gendata():
            """
            Получение генератора json для использования в пакетной загрузке в Elasticsearch
            """
            for row in data:
                yield {"_index": index_name, "_id": row["id"], "_source": row}

        result = helpers.bulk(es_client, gendata(), stats_only=True)
        log.debug(result)
        log.info("Batch loaded")


def create_es_index(
    es_client: Elasticsearch,
    index_name: str,
    index_schema: str,
) -> None:
    """Функция для создания индекса, если уже не существует индекса с таким именем"""
    if not es_client.indices.exists(index=index_name):
        with open(index_schema) as f:
            schema = json.loads(f.read())
            result = es_client.indices.create(index=index_name, **schema)
            log.debug(result)
            log.info("Created index with name: {}".format(index_name))


def load_to_es(
    es_client: Elasticsearch,
    pg_connection: _connection,
) -> None:
    """Основной метод для инкрементальной миграции данных из БД Postgres в индекс movies в Elasticsearch"""
    INDEX_NAME = "movies"
    STATE_STORAGE = "state_storage.json"
    INDEX_SCHEMA = "es_schema.json"
    BATCH_SIZE = 100

    pg_extractor = extractor.PgExtractor(pg_connection)

    # Создаем индекс
    create_es_index(es_client, INDEX_NAME, INDEX_SCHEMA)

    # Получаем последнее состояние из файла
    file_storage = state.JsonFileStorage(STATE_STORAGE)
    curr_state = state.State(file_storage)
    last_modified = curr_state.get_state("modified")

    # Если текущее состояние в файле не найдено, считаем загрузку первоначальной и выгружаем все кинопроизведения.
    #
    # Для ревьюера: не считаю целесообразным на данный момент переносить обновление состояния в load_batches.
    # load_batches используется как для первоначального заполнения индекса данными, так и для последующего инкрементального их обновления.
    # При этом состояние при первоначальной и последующих загрузках определяется по-разному:
    #   - при initial загрузке берется максимальная дата modified в person, genre, film_work.
    #     Выбирается отдельным агрегирующим запросом по трем таблицам к БД (PgExtractor.get_last_modified())
    #   - при последующих обновлениях берется максимальная дата modified из отобранных для обновления кинопроизведений.
    #     Выбирается тем же запросом, что и список кинопроизведений для обновления
    # Для того, чтобы не потерять данные при initial загрузке, перенес получение состояния в начало загрузки.
    # В этом случае, если между получением состояния и записью последнего пакета в ES, появятся новые изменения, они будут обработаны следующей итерацией загрузки
    if not last_modified:
        log.info("Initial load started.")
        last_modified = str(pg_extractor.get_last_modified())
        pg_extractor.select_all_movies()
        load_batches(es_client, pg_extractor, BATCH_SIZE, INDEX_NAME)
        log.info("last_modified = {}".format(last_modified))
    # Если в файле есть состояние, загружаем фильмы, обновленные позже даты из файла
    # Состояние, которое будет установленно в качестве текущего для следующей итерации,
    # изменится на максимальную дату modified из выбранных для обновления кинопроизведений.
    # Если с момента предыдущей итерации данные не изменились, состояние останется прежним
    else:
        log.info("Last modified = {}".format(last_modified))
        movies = pg_extractor.get_movies_to_update(
            datetime.fromisoformat(last_modified)
        )
        if movies:
            ids = tuple(row["id"] for row in movies)
            pg_extractor.select_movies(ids)
            load_batches(es_client, pg_extractor, BATCH_SIZE, INDEX_NAME)
            last_modified = str(max(row["modified"] for row in movies))
            log.info("last_modified = {}".format(last_modified))

    # Сохраняем новое значения состояния в файл
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
                "Next sync iteration in {} seconds.".format(
                    os.environ.get("LOOP_TIMEOUT")
                )
            )
        time.sleep(int(os.environ.get("LOOP_TIMEOUT")))
