from peewee import Model, CharField, DoubleField, ForeignKeyField, PrimaryKeyField, DateTimeField, IntegerField
from peewee_asyncext import PooledPostgresqlExtDatabase
from playhouse.postgres_ext import JSONField
from playhouse.shortcuts import model_to_dict, dict_to_model

import cfg

ACTUAL_VERSION = 1

connection = PooledPostgresqlExtDatabase(
    cfg.db.database,
    host=cfg.db.host,
    port=cfg.db.port,
    user=cfg.db.user,
    password=cfg.db.password,
    register_hstore=False
)


class BasicModel(Model):
    id = PrimaryKeyField()

    class Meta:
        database = connection

    # def __str__(self):
    #     if hasattr(self, "name"):
    #         name = self.name
    #     else:
    #         name = self.id
    #
    #     return "<{} {}>".format(self.__class__.__name__, name)

    def dict(self, recurse=False, **kwargs):
        """
        Преобразовать peewee объект в словарь
        :return: dict
        """
        body = model_to_dict(self, recurse, **kwargs)
        return body

    @classmethod
    def from_dict(cls, d, ignore_unknown=True):
        """
        Собрать модель из словаря
        :param d: словарь с полями соответствующими модели
        :param ignore_unknown: игнорировать поля словаря, которых нет у модели
        :return: модель
        """
        model = dict_to_model(cls, d, ignore_unknown=ignore_unknown)
        return model

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    def update_from_dict(self, d):
        for k, v in d.items():
            setattr(self, k, v)


class Schema(BasicModel):
    key = CharField(unique=True, null=True)
    value = CharField(null=True)


class User(BasicModel):
    """
    Пользователь
    """
    login = CharField(unique=True)
    email = CharField(null=True, unique=True)
    phone = CharField(null=True)
    password_hash = CharField(null=True)
    first_name = CharField(default="")
    last_name = CharField(null=True)
    role = CharField(null=True, default="user")
    picture = JSONField(null=True)
    status = CharField(default="disabled")
    code = CharField(null=True)
    rating = DoubleField(default=0)
    reg_method = CharField(null=True)
    registration_date = DateTimeField(null=True)
    money = IntegerField(default=0)
    lang = CharField(default="en")
    timezone = CharField(default="UTC")

    def __str__(self):
        return "{id}: {first_name} {last_name} ({pair})".format(**self.dict())


class Social(BasicModel):
    """
    Подключенные социальные сети
    """
    user = ForeignKeyField(User, on_delete="CASCADE")
    network = CharField(null=True)
    social_id = CharField(null=True, max_length=1000)
    access_token = CharField(null=True, max_length=1000)


class Session(BasicModel):
    """
    Сессия пользователя
    """
    user = ForeignKeyField(User, on_delete="CASCADE")
    token = CharField(null=True, index=True)
    rand = CharField(null=True)
    expire = DateTimeField(null=True)
    client = CharField(null=True)
    os = CharField(null=True)
    user_agent = CharField(null=True)
    login_method = CharField(null=True)
    push_token = CharField(null=True, max_length=2000)
    ip = CharField(null=True)
    location = JSONField(null=True)
    last_login = DateTimeField(null=True)


class HandlerStat(BasicModel):
    endpoint = CharField()
    handler = CharField()
    method = CharField()
    handler_version = IntegerField(null=True)
    api_version = IntegerField()
    updated = DateTimeField()
    counter = IntegerField(default=1)
    last_ip = CharField(null=True)
