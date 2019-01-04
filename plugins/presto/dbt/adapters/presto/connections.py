from contextlib import contextmanager

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from pyhive import presto


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
    'required': ['database', 'schema', 'host', 'port'],
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
    @contextmanager
    def exception_handler(self, sql, connection_name='master'):
        try:
            yield
        # TODO: introspect into `DatabaseError`s and expose `errorName`,
        # `errorType`, etc instead of stack traces full of garbage!
        except Exception as exc:
            logger.debug("Error while running:\n{}".format(sql))
            logger.debug(exc)
            raise dbt.exceptions.RuntimeException(dbt.compat.to_string(exc))

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = connection.credentials

        # it's impossible for presto to fail here as 'connections' are actually
        # just cursor factories.
        handle = presto.connect(
            host=credentials.host,
            port=credentials.get('port', 8080),
            username=credentials.get('username'),
            password=credentials.get('password'),
            catalog=credentials.database
        )
        connection.state = 'open'
        connection.handle = handle
        return connection

    @classmethod
    def get_status(cls, cursor):
        return 'OK'

    def cancel(self, connection):
        pass
