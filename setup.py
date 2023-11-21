from setuptools import setup, find_packages

setup(
    name='yeedu_operator_test',
    version='0.0.2',
    packages=find_packages(),
    install_requires=[
        'apache-airflow>=2.7.3',
        # Add any other dependencies here
    ],
)

