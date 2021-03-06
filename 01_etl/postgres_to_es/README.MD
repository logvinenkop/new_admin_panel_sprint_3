## Сервис для переноса данных из Postgres в Elasticsearch
Сервис обеспечивает:
- Первоначальное создание и наполнение пустого индекса данными из Postgres в соответствие со [cхемой индекса](https://code.s3.yandex.net/middle-python/learning-materials/es_schema.txt) `movies`
- Дальнейший инкрементальный перенос измененных данных с заданной периодичностью
- Отказоустойчивость при недоступности Postgres или Elasticsearch при помощи backoff
- Сохранение состояния на момент остановки или последнего обновления для последуюшего возобновления работы с места остановки

### Запуск
- Установить requirements.txt
- Создать файл `.env` в директории проекта. В нем здать следующие переменные окружения:
    - DB_NAME=`<Имя БД>`
    - DB_USER=`<Пользователь БД>`
    - DB_PASSWORD=`<Пароль БД>`
    - DB_HOST=`<Адрес хоста БД>`
    - DB_PORT=`<Порт БД>`
    - ES_URL=`<URL Elasticsearch>`
    - LOOP_TIMEOUT=`<Таймаут синхронизации>`
- Запустить основной файл `loader.py`

При первом запуске в директории проекта будет создан файл `state_storage.json`, в котором будет сохранено состояние синхронизации.
