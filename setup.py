from setuptools import setup, find_packages

setup(
    name='airflow-yeedu-operator',
    version='1.0.0',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.5.0',
        'requests>=2.27',
        # Add any other dependencies here
    ],
    project_urls={
        'GitHub': 'https://github.com/rohith-sagar/AirflowYeeduOperator.git',
        # Add more URLs as needed
    },
)

