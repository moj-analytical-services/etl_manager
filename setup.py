from setuptools import setup, find_packages

setup(
    name='etl_manager',
    version='1.0.2',
    packages=find_packages(exclude=['tests*']),
    license='MIT',
    description='A python package to manage etl processes on AWS',
    long_description=open('README.md').read(),
    install_requires=[
        "boto3 >= 1.7.4",
        "PyAthenaJDBC >= 1.3.0",
        "jsonschema >= 2.6.0"
    ],
    include_package_data=True,
    url='https://github.com/moj-analytical-services/etl_manager',
    author='Karik Isichei',
    author_email='karik.isichei@digital.justice.gov.uk'
)