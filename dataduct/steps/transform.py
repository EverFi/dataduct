"""
ETL step wrapper for shell command activity can be executed on Ec2 / EMR
"""
from ..pipeline import S3Node
from ..pipeline import ShellCommandActivity
from ..pipeline import SNSAlarm
from ..s3 import S3Directory
from ..s3 import S3File
from ..s3 import S3Path
from ..config import Config
from ..utils import constants as const
from ..utils.exceptions import ETLInputError
from ..utils.helpers import exactly_one
from ..utils.helpers import get_modified_s3_path
from .etl_step import ETLStep

import logging
logger = logging.getLogger(__name__)
config = Config()

SCRIPT_ARGUMENT_TYPE_STRING = 'string'
SCRIPT_ARGUMENT_TYPE_SQL = 'sql'
SNS_TOPIC_ARN_SUCCESS = config.etl.get('SNS_TOPIC_ARN_SUCCESS', const.NONE)
SNS_TOPIC_ARN_FAILURE = config.etl.get('SNS_TOPIC_ARN_FAILURE', const.NONE)
SNS_TOPIC_ARN_ONLATE = config.etl.get('SNS_TOPIC_ARN_ONLATE', const.NONE)


class TransformStep(ETLStep):
    """Transform Step class that helps run scripts on resources
    """

    def __init__(self,
                 command=None,
                 script=None,
                 script_uri=None,
                 script_directory=None,
                 script_name=None,
                 script_arguments=None,
                 additional_s3_files=None,
                 output_node=None,
                 output_path=None,
                 no_output=False,
                 no_input=False,
                 sns_include_default=False,
                 sns_success_subject=None,
                 sns_failure_subject=None,
                 sns_onlate_subject=None,
                 send_sns=False,
                 send_sns_success=True,
                 send_sns_late=True,
                 send_sns_fail=True,
                 sns_message=None,
                 precondition=None,
                 **kwargs):
        """Constructor for the TransformStep class

        Args:
            command(str): command to be executed directly
            script(path): local path to the script that should executed
            script_directory(path): local path to the script directory
            script_name(str): script to be executed in the directory
            script_arguments(list of str): list of arguments to the script
            additional_s3_files(list of S3File): additional files used
            output_node(dict): output data nodes from the transform
            output_path(str): the S3 path to output data
            no_output(bool): whether the script outputs anything to s3
            no_input(bool): whether the script takes any inputs
            sns_include_default(bool): default false.  whether default message should be appended to the one specified by the pipeline (default message is different for each action - see dataduct/pipeline/sns_alarm.py:38)
            sns_success_subject(str): default None. subject defined for sns success action, if None a default subject will be defined later on dataduct/pipeline/sns_alarm.py:130
            sns_failure_subject(str): default None. subject defined for sns failure action, if None a default subject will be defined later on dataduct/pipeline/sns_alarm.py:116
            sns_onlate_subject(str): default None. subject defined for sns onlate action, if None a default subject will be defined later on dataduct/pipeline/sns_alarm.py:103
            send_sns(bool): default False. whether SNS action should be sent for any of the possible events (success, failure, late)
            send_sns_success(bool): default True. whether SNS action should be sent for success action. if true "send_sns" should be set to true
            send_sns_late(bool): default True. whether SNS action should be sent for late action. if true "send_sns" should be set to true
            send_sns_fail(bool): default True. whether SNS action should be sent for fail action. if true "send_sns" should be set to true
            sns_message(str): default None. Message to be sent on all sns notifications. Mandatory if send_sns=True
            **kwargs(optional): Keyword arguments directly passed to base class
        """
        super(TransformStep, self).__init__(**kwargs)

        if not exactly_one(command, script, script_uri, script_directory):
            raise ETLInputError('Only one of script, script_uri, command' +
                                ' and directory allowed')

        # Create output_node based on output_path
        if no_output:
            base_output_node = None
        else:
            base_output_node = self.create_s3_data_node(
                self.get_output_s3_path(get_modified_s3_path(output_path)))

        script_arguments = self.translate_arguments(script_arguments)
        if script_arguments is None:
            script_arguments = []

        if self.input and not no_input:
            input_nodes = [self.input]
        else:
            input_nodes = []

        if script_directory:
            # The script to be run with the directory
            if script_name is None:
                raise ETLInputError('script_name required with directory')

            script_directory = self.create_script(
                S3Directory(path=script_directory))

            # Input node for the source code in the directory
            input_nodes.append(self.create_pipeline_object(
                object_class=S3Node,
                schedule=self.schedule,
                s3_object=script_directory
            ))

            # We need to create an additional script that later calls the main
            # script as we need to change permissions of the input directory
            ip_src_env = 'INPUT%d_STAGING_DIR' % (1 if not self.input else 2)
            additional_args = ['--INPUT_SRC_ENV_VAR=%s' % ip_src_env,
                               '--SCRIPT_NAME=%s' % script_name]

            script_arguments = additional_args + script_arguments
            command = const.SCRIPT_RUNNER_COMMAND

        # Create S3File if script path provided
        if script:
            script = self.create_script(S3File(path=script))
        elif script_uri:
            script = S3File(s3_path=S3Path(uri=script_uri))

        # Translate output nodes if output path is provided
        if output_node:
            self._output = self.create_output_nodes(
                base_output_node, output_node)
        else:
            self._output = base_output_node

        logger.debug('Script Arguments:')
        logger.debug(script_arguments)

        output_node = None if no_output else base_output_node

        #sns stuff

        if send_sns:
            sns_message.update({'step_name': self.get_name()})
            if send_sns_fail:
                self._sns_object = self.create_pipeline_object(
                    object_class=SNSAlarm,
                    topic_arn=SNS_TOPIC_ARN_FAILURE,
                    failure_subject=sns_failure_subject,
                    my_message=sns_message,
                    include_default_message=sns_include_default,
                    failure=True,
                    onlate=False,
                )
            if send_sns_success:
                self._sns_success_object = self.create_pipeline_object(
                    object_class=SNSAlarm,
                    topic_arn=SNS_TOPIC_ARN_SUCCESS,
                    success_subject=sns_success_subject,
                    my_message=sns_message,
                    include_default_message=sns_include_default,
                    failure=False,
                )
            if send_sns_late:
                self._sns_onlate_object = self.create_pipeline_object(
                    object_class=SNSAlarm,
                    topic_arn=SNS_TOPIC_ARN_ONLATE,
                    onlate_subject=sns_onlate_subject,
                    my_message=sns_message,
                    include_default_message=sns_include_default,
                    failure=True,
                    onLate=True,
                )

        self.create_pipeline_object(
            object_class=ShellCommandActivity,
            input_node=input_nodes,
            output_node=output_node,
            resource=self.resource,
            worker_group=self.worker_group,
            schedule=self.schedule,
            script_uri=script,
            script_arguments=script_arguments,
            command=command,
            max_retries=self.max_retries,
            depends_on=self.depends_on,
            additional_s3_files=additional_s3_files,
            precondition=precondition,
        )

    def translate_arguments(self, script_arguments):
        """Translate script argument to lists

        Args:
            script_arguments(list of str/dict): arguments to the script

        Note:
            Dict: (k -> v) is turned into an argument "--k=v"
            List: Either pure strings or dictionaries with name, type and value
        """
        if script_arguments is None:
            return script_arguments

        elif isinstance(script_arguments, list):
            result = list()
            for argument in script_arguments:
                if isinstance(argument, dict):
                    result.extend([
                        self.input_format(key, get_modified_s3_path(value))
                        for key, value in argument.items()
                    ])
                else:
                    result.append(get_modified_s3_path(str(argument)))
            return result

        elif isinstance(script_arguments, dict):
            return [self.input_format(key, get_modified_s3_path(value))
                    for key, value in script_arguments.items()]

        elif isinstance(script_arguments, str):
            return [get_modified_s3_path(script_arguments)]

        else:
            raise ETLInputError('Script Arguments for unrecognized type')

    @staticmethod
    def input_format(key, value):
        """Format the key and value to command line arguments
        """
        return ''.join(['--', key, '=', value])

    @classmethod
    def arguments_processor(cls, etl, input_args):
        """Parse the step arguments according to the ETL pipeline

        Args:
            etl(ETLPipeline): Pipeline object containing resources and steps
            step_args(dict): Dictionary of the step arguments for the class
        """
        if input_args.pop('resource_type', None) == const.EMR_CLUSTER_STR:
            resource_type = const.EMR_CLUSTER_STR
        else:
            resource_type = const.EC2_RESOURCE_STR
        step_args = cls.base_arguments_processor(
            etl, input_args, resource_type=resource_type)

        return step_args
