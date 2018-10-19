"""Constants shared across dataduct
"""

# Constants
ZERO = 0
ONE = 1
NONE = None
EMPTY_STR = ''
NULL_STR = 'NULL'
DEFAULT_DELAY = '10 Minutes'
DEFAULT_TIMEOUT = '6 Hours'

# ETL Constants
EMR_CLUSTER_STR = 'emr'
EC2_RESOURCE_STR = 'ec2'
M1_LARGE = 'm1.large'

LOG_STR = 'logs'
DATA_STR = 'data'
SRC_STR = 'src'
QA_STR = 'qa'

# Commands
COMMAND_TEMPLATE = 'python -c "from {file} import {func}; {func}()" "$@"'

COUNT_CHECK_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.count_check',
    func='count_check')

COLUMN_CHECK_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.column_check',
    func='column_check')

LOAD_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.create_load_redshift',
    func='create_load_redshift_runner')

PK_CHECK_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.primary_key_check',
    func='primary_key_check')

DEPENDENCY_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.dependency_check',
    func='dependency_check')

SCRIPT_RUNNER_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.runner', func='script_runner')

SQL_RUNNER_COMMAND = COMMAND_TEMPLATE.format(
    file='dataduct.steps.executors.runner', func='sql_runner')

FREQUENCY_PERIOD_CONVERSION = {
    'weekly': ('1 week', None),
    '1-week': ('1 week', None),
    '2-weeks': ('2 weeks', None),
    'daily': ('1 day', None),
    '1-day': ('1 day', None),
    '2-days': ('2 days', None),
    '3-days': ('3 days', None),
    '4-days': ('4 days', None),
    '5-days': ('5 days', None),
    '6-days': ('6 days', None),
    '15-minutes': ('15 minutes', None),
    'hourly': ('1 hour', None),
    '1-hour': ('1 hour', None),
    '2-hours': ('2 hours', None),
    '3-hours': ('3 hours', None),
    '4-hours': ('4 hours', None),
    '6-hours': ('6 hours', None),
    '8-hours': ('8 hours', None),
    '12-hours': ('12 hours', None),
    '15-hours': ('15 hours', None),
    'one-time': ('15 minutes', 1),
}

DEFAULT_ATTEMPT_TIMEOUT = '1-hour'
DEFAULT_ONLATE_TIMEOUT = '15-minutes'

