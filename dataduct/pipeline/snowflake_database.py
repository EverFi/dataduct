"""
Pipeline object class for snowflake Jdbc database
see http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-jdbcdatabase.html
"""

from ..config import Config
from .pipeline_object import PipelineObject
from ..utils.exceptions import ETLConfigError
from IPython import embed

config = Config()
if not hasattr(config, 'snowflake'):
    raise ETLConfigError('Snowflake credentials missing from config')

class SnowflakeDatabase(PipelineObject):
    """Jdbc resource class
    """

    def __init__(self,
                 id,
                 account_name=None,
                 role=None,
                 database=None,
                 warehouse=None,
                 username=None,
                 jdbc_driver_uri=None,
                 password=None):
        """Constructor for the Snowflake class

        Args:
            id(str): id of the object
            host(str):
            port(str):
            database(str):
            jdbc_driver_uri(str):
            username(str): username for the database
            password(str): password for the database
        """

        if (None in [ jdbc_driver_uri, username, password]):
            raise ETLConfigError('Snowflake credentials missing from config')

        connection_string = "jdbc:snowflake://" + account_name + ".snowflakecomputing.com?" + "role=" + role + "&warehouse=" + warehouse + "&db=" + database
        jdbc_driver_class = "net.snowflake.client.jdbc.SnowflakeDriver"

        kwargs = {
            'id': id,
            'type': 'JdbcDatabase',
            'connectionString': connection_string,
            'jdbcDriverClass': jdbc_driver_class,
            'jdbcDriverJarUri': jdbc_driver_uri,
            'username': username,
            '*password': password,
        }
        super(SnowflakeDatabase, self).__init__(**kwargs)

