from setuptools import setup, find_packages

setup(
    name='yeedu_job_run_operator_test',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.7.3',
        # Add any other dependencies here
    ],
    project_urls={
        'GitHub': 'https://github.com/rohith-sagar/AirflowYeeduOperator.git',
        # Add more URLs as needed
    },
)

