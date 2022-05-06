import abc
import json
from typing import Any, Optional
from pathlib import Path


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class JsonFileStorage(BaseStorage):
    """Класс для реализации сохранения и получения состояния из хранилища в json файле"""

    def __init__(self, file_path: Optional[str] = None):
        self.file_path = file_path
        file = Path(self.file_path)
        file.touch(exist_ok=True)
        self.file_isempty = file.stat().st_size == 0

    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        with open(self.file_path) as f:
            if not self.file_isempty:
                state_json = json.loads(f.read())
                return state_json
            return {}

    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        with open(self.file_path, "w") as f:
            json.dump(state, f)


class State:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или распределённым хранилищем.
    """

    def __init__(self, storage: JsonFileStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        state_dict = {key: value}
        self.storage.save_state(state_dict)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        state_dict = self.storage.retrieve_state()
        return state_dict.get(key)
