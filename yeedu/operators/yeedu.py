"""This module contains Yeedu operators."""
from typing import Optional, Tuple, Union
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from yeedu.operators.yeedu import YeeduHook
from airflow.exceptions import AirflowException
from airflow.models import Variable  # Import Variable from airflow.models

class YeeduJobRunOperator(BaseOperator):
    """
    YeeduJobRunOperator submits a job to Yeedu and waits for its completion.

    :param job_conf_id: The job configuration ID.
    :type job_conf_id: str
    :param token: Yeedu API token. If not provided, it will be retrieved from Airflow Variables.
    :type token: str, optional
    :param hostname: Yeedu API hostname. If not provided, it will be retrieved from Airflow Variables.
    :type hostname: str, optional

    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """

    template_fields: Tuple[str] = ("job_id",)

    @apply_defaults
    def __init__(
        self,
        job_conf_id: str,
        token: Optional[str] = None,
        hostname: Optional[str] = None,
        workspace_id: Optional[int] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.job_conf_id: str = job_conf_id
        self.token: str = token or Variable.get("yeedu_token")  # You can use Airflow Variable to store sensitive information
        self.hostname: str = hostname
        self.workspace_id: int = workspace_id
        self.hook: YeeduHook = YeeduHook(token=self.token, hostname=self.hostname, workspace_id=self.workspace_id)
        self.job_id: Optional[Union[int, None]] = None

    def execute(self, context: dict) -> None:
        """
        Execute the YeeduOperator.

        - Get the job ID.
        - Wait for the job to complete.
        - Retrieve and log job logs.

        :param context: The execution context.
        :type context: dict
        """
        job_id = self.hook.submit_job(self.job_conf_id)

        self.log.info("JOB ID: %s", job_id)
        job_status: str = self.hook.wait_for_completion(job_id)
        self.log.info("FINAL JOB STATUS: %s", job_status)

        if job_status in ['DONE']:
            log_type: str = 'stdout'
        elif job_status in ['ERROR', 'TERMINATED']:
            log_type: str = 'stderr'
        else:
            self.log.error("Job completion status is unknown.")
            return

        job_log: str = self.hook.get_job_logs(job_id, log_type)
        self.log.info("Logs for Job ID %s (Log Type: %s): %s", job_id, log_type, job_log)

        if job_status in ['ERROR', 'TERMINATED']:
            raise AirflowException(job_log)
                        

