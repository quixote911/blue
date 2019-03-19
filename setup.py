from setuptools import setup, find_packages

setup(
    name='blue_event_framework',
    version='0.2.1',
    author='Coinswitch',
    author_email='dev@coinswitch.co',
    description='dummy description',
    packages=find_packages(),
    install_requires=[
        'dataclasses>=0.6',
        'peewee>=3.9.2',
        'boto3>=1.7'
    ]
)