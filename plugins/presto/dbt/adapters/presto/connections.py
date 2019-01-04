from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager


PRESTO_CREDENTIALS_CONTRACT = {
    'type': 'object',
    'additionalProperties': False,
    'properties': {
        'database': {
            'type': 'string',
        },
        'schema': {
            'type': 'string',
        },
        'host': {
            'type': 'string',
        },
        'port': {
            'type': 'integer',
            'minimum': 0,
            'maximum': 65535,
        },
        'username': {
            'type': 'string',
        },
        'password': {
            'type': 'string',
        },
    },
    'required': ['database', 'schema', 'host'],
}


class PrestoCredentials(Credentials):
    SCHEMA = PRESTO_CREDENTIALS_CONTRACT

    def _connection_keys(self):
        return ('host', 'port', 'database', 'username')


class PrestoConnectionManager(SQLConnectionManager):
    TYPE = 'presto'
    ALIASES = {
        'catalog': 'database',
    }
