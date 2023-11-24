"""This module contains Yeedu Hooks."""

import requests
import time
import logging
from typing import Tuple
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import sys

class YeeduHook(BaseHook):
    """
    YeeduHook provides an interface to interact with the Yeedu API.

    :param token: Yeedu API token.
    :type token: str
    :param hostname: Yeedu API hostname.
    :type hostname: str
    :param args: Additional positional arguments.
    :param kwargs: Additional keyword arguments.
    """

    def __init__(self, token: str, hostname: str, workspace_id: int, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.token: str = token
        self.headers: dict = {
            'accept': 'application/json',
            'Authorization': f"Bearer {token}",
            'Content-Type': 'application/json'
        }
        self.base_url: str = f'http://{hostname}/api/v1/workspace/{workspace_id}/'

    def _api_request(self, method: str, url: str, data=None) -> requests.Response:
        """
        Make an API request.

        :param method: HTTP method (GET, POST, etc.).
        :type method: str
        :param url: API endpoint URL.
        :type url: str
        :param data: JSON data for the request.
        :type data: dict, optional
        :return: The API response.
        :rtype: requests.Response
        """
        response: requests.Response = requests.request(method, url, headers=self.headers, json=data)
        return response
            


    def submit_job(self, job_conf_id: str) -> int:
        """
        Submit a job to Yeedu.

        :param job_conf_id: The job configuration ID.
        :type job_conf_id: str
        :return: The JSON response from the API.
        :rtype: int
        """
        try:
            job_url: str = self.base_url + 'spark/job'
            data: dict = {'job_conf_id': job_conf_id}
            response = self._api_request('POST', job_url, data)
            api_status_code = response.status_code
            if api_status_code == 200:
                job_id = response.json().get('job_id')
                if job_id:
                    return job_id
                else:
                    raise AirflowException(response)
            else:
                raise AirflowException(response)
                
        except Exception as e:
            raise AirflowException(e)
            


    def get_job_status(self, job_id: int) -> requests.Response:
        """
        Get the status of a Yeedu job.

        :param job_id: The job ID.
        :type job_id: int
        :return: The API response.
        :rtype: requests.Response
        """
        job_status_url: str = self.base_url + f'spark/job/{job_id}'
        return self._api_request('GET', job_status_url)

            

    def get_job_logs(self, job_id: int, log_type: str) -> str:
        """
        Get the logs for a Yeedu job.

        :param job_id: The job ID.
        :type job_id: int
        :param log_type: The type of logs to retrieve ('stdout' or 'stderr').
        :type log_type: str
        :return: The job logs.
        :rtype: str
        """
        try:
            logs_url: str = self.base_url + f'spark/job/{job_id}/log/{log_type}'
            time.sleep(10)
            return self._api_request('GET', logs_url).text
        except Exception as e:
            raise AirflowException(e)
            

    def wait_for_completion(self, job_id: int) -> str:
        """
        Wait for the completion of a Yeedu job.

        :param job_id: The job ID.
        :type job_id: int
        :return: The final job status.
        :rtype: str
        :raises AirflowException: If continuous API failures reach the threshold.
        """
        try:
            max_attempts: int = 5
            attempts_failure: int = 0

            while True:
                time.sleep(5)
                # Check job status
                response: requests.Response = self.get_job_status(job_id)
                api_status_code: int = response.status_code
                self.log.info("Current API Status Code: %s",api_status_code)
                if api_status_code == 200:
                    # If API status is a success, reset the failure attempts counter
                    attempts_failure = 0
                    job_status: str = response.json().get('job_status')
                    self.log.info("Current Job Status: %s ", job_status)
                    if job_status in ['DONE', 'ERROR', 'TERMINATED', 'KILLED']:
                        break
                # If API status is an error, increment the failure attempts counter
                else:
                    attempts_failure += 1
                    self.log.info("failure attempts : %s", attempts_failure)

                # If continuous failures reach the threshold, throw an error
                if attempts_failure == max_attempts:
                    raise AirflowException("Continuous API failure reached the threshold")

            return job_status
        except Exception as e:
            raise AirflowException(e)