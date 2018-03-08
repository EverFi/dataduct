"""
Pipeline object class for sns
"""

from ..config import Config
from ..utils import constants as const
from .pipeline_object import PipelineObject
import json

config = Config()
SNS_TOPIC_ARN_FAILURE = config.etl.get('SNS_TOPIC_ARN_FAILURE', const.NONE)
SNS_TOPIC_ARN_SUCCESS = config.etl.get('SNS_TOPIC_ARN_SUCCESS', const.NONE)
ROLE = config.etl['ROLE']


class SNSAlarm(PipelineObject):
    """SNS object added to all pipelines
    """

    def __init__(self,
                 id,
                 my_message=None,
                 topic_arn=None,
                 success_subject=None,
                 failure_subject=None,
                 include_default_message=False,
                 failure=True,
                 **kwargs):
        """Constructor for the SNSAlarm class

        Args:
            id(str): id of the object
            my_message(dict): Message used in SNS,
            **kwargs(optional): Keyword arguments directly passed to base class
        """

        default_failure_message = {
                 'pipeline_object': '#{node.name}',
                 'schedule_start_time': '#{node.@scheduledStartTime}',
                 'error_message': '#{node.errorMessage}',
                 'error_stack_trace': '#{node.errorStackTrace}'
        }

        default_success_message = {
                 'pipeline_object': '#{node.name}',
                 'pipeline_object_scheduled_start_time': '#{node.@scheduledStartTime}',
                 'pipeline_object_actual_start_time': '#{node.@actualStartTime}',
                 'pipeline_object_actual_end_time': '#{node.@actualEndTime}'
        }

        if failure:
            if not my_message:
                my_message = default_failure_message
            elif my_message and include_default_message:
                my_message.update(default_failure_message)
            if failure_subject:
                subject=failure_subject
            else:
                subject = 'Data Pipeline Failed'

            if topic_arn is None:
                topic_arn = SNS_TOPIC_ARN_FAILURE

        else:
            if not my_message:
                my_message = default_success_message
            elif my_message and include_default_message:
                my_message.update(default_success_message)

            if success_subject:
                subject=success_subject
            else:
                subject = 'Data Pipeline Succeeded'

            if topic_arn is None:
                topic_arn = SNS_TOPIC_ARN_SUCCESS

        super(SNSAlarm, self).__init__(
            id=id,
            type='SnsAlarm',
            topicArn=topic_arn,
            role=ROLE,
            subject=subject,
            message=json.dumps(my_message),
        )
