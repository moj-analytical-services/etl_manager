from setuptools import setup, find_packages

setup(
    name='etl_manager',
    version='1.0.0',
    packages=find_packages(exclude=['tests*']),
    license='MIT',
    description='A python package to manage etl processes on AWS',
    long_description=open('README.md').read(),
    install_requires=[],
    include_package_data=True,
    url='https://github.com/moj-analytical-services/etl_manager',
    author='Karik Isichei',
    author_email='karik.isichei@digital.justice.gov.uk'
)
