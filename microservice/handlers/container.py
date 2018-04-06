import logging
from asyncio import iscoroutine
from collections import UserDict
from datetime import datetime, timezone, date, time, timedelta

from microservice.exceptions import CurvedHands


class Data:
    """
    Контейнер данных, возвращаемый менеджером. Приводит результат к словарю для сериализации.
    Обязательно должен содержать result или error.
    """
    def __init__(self, result=None, error=None, meta=None, need_format=True, reformat=True, transpose=None, **kwargs):
        """
        Создать контейнер на основе объекта Model
        :param result: результат запроса peewee (dict or list)
        :param error: текст ошибки
        :param meta: метаданные от менеджера
        :param need_format: нужно применить format
        :param reformat: применить метод reformat
        :param transpose: преобразовать в словарь (транспонированием)
        """
        assert result is not None or error is not None
        self.kwargs = kwargs
        self.meta = dict()
        self.error = error
        if not error:
            if type(result) is dict and "hits" in result:
                # elastic results
                self.data = [x["_source"] for x in result["hits"]["hits"]]
                if reformat:
                    self.data = list(map(self.reformat, self.data))
                if transpose:
                    self.transpose(transpose[0], transpose[1])
                self.count = len(self.data)
                self.meta["shown"] = self.count
                self.meta["total"] = result["hits"]["total"]
                self.data = self._cast(self.data)
            elif type(result) is list:
                # list of objects
                if need_format:
                    self.data = list(map(self.format, result))
                else:
                    self.data = result
                if reformat:
                    self.data = list(map(self.reformat, self.data))
                if transpose:
                    self.transpose(transpose[0], transpose[1])
                self.count = len(self.data)
                self.meta["shown"] = self.count
                self.data = self._cast(self.data)
            elif type(result) is dict or isinstance(result, UserDict):
                # single object
                if need_format:
                    self.data = self.format(result)
                else:
                    self.data = result
                if reformat:
                    self.data = self.reformat(self.data)
                self.count = 1
                self.meta["shown"] = self.count
                self.data = self._cast(self.data)
            elif iscoroutine(result):
                raise CurvedHands("You forgot 'await' statement")
            else:
                self.data = result
                logging.debug("Data is is unknown format: {}".format(self.data))
                self.error = "Data is is unknown format"
            if meta:
                self.meta.update(meta)
        else:
            self.data = None

    @staticmethod
    def format(item):
        """
        Задать интерфейс объекта. Метод для переопределния.
        :param item: dict соответствующий модели
        :return: dict для вывода (эластик)
        """
        return item

    @staticmethod
    def reformat(item):
        """
        Выполнить переформатирование готового объекта
        :param item: dict для вывода (эластик)
        :return: dict измененный
        """
        return item

    @classmethod
    def list(cls, items):
        """
        Сделать список используя self.format. Используется в дочерних классах, пример:
            def format(item):
                item = {
                    "users": ContributorsData.list(item["users"]),
                    "users_count": item["users_count"],
                }
                return item
        :param items: список для форматирования
        :return: форматированный список
        """
        if items is not None:
            items = list(map(cls.format, items))
        else:
            items = []
        return items

    def _cast(self, value):
        """
        Приводит значение к виду удобному для сериализации
        Если значение является словарем или списком, преобразует рекурсивно
        """
        # преобразовать определенные типы (добавить нужное)
        if type(value) in (datetime, time):
            return value.replace(tzinfo=timezone.utc).isoformat()
        elif type(value) is date:
            return value.replace().isoformat()
        elif type(value) is timedelta:
            return value.days
        elif iscoroutine(value):
            raise CurvedHands("You forgot 'await' statement")

        # рекурсивно пройтись по элементам списка или словаря
        elif type(value) is dict:
            return {k: self._cast(v) for k, v in value.items()}
        elif type(value) is list:
            return [self._cast(v) for v in value]
        else:
            return value

    def transpose(self, key, value=None):
        """
        Превратить список элементов в словарь с ключом key (одно из полей)
        :param key: поле для использования в качестве ключа, должно быть уникальным, иначе будет потеря данных
        :param value: оставить только это поле в качестве значения элементов словаря. Если не указано, то значение
            словаря это словарь со всеми остальными полями
        """
        if value:
            self.data = {x[key]: x[value] for x in self.data}
        else:
            self.data = {x[key]: x for x in self.data}

    def group_by(self, field):
        """
        Превратить список элементов в словарь, в котором элементы сгруппированы по полю field, аналог group by в sql
        ключом является значение поля field
        значением является список элементов, у которых было определенное поле field
        количество групп - количество уникальных значений field
        :param field: поле для группировки
        """
        groups = {}
        for item in self.data:
            if not item[field] in groups:
                groups[item[field]] = []
            groups[item[field]].append(item)
        self.data = groups

    def __str__(self):
        return "<{} {}>".format(self.__class__.__name__, self.data)