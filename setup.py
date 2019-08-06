from setuptools import setup, find_packages

setup(
    name="etl_manager",
    version="4.0.0",
    packages=find_packages(exclude=["tests*"]),
    license="MIT",
    description="A python package to manage etl processes on AWS",
    long_description=open("README.md").read(),
    install_requires=[
        "boto3 >= 1.7.4",
        "jsonschema >= 2.6.0",
        "parameterized==0.7.0",
        "regex==2019.6.8",
    ],
    include_package_data=True,
    url="https://github.com/moj-analytical-services/etl_manager",
    author="Karik Isichei",
    author_email="karik.isichei@digital.justice.gov.uk",
)
