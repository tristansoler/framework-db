from setuptools import setup

with open("README", 'r') as f:
    long_description = f.read()

setup(
   name='data_framework',
   version='1',
   description='Sample',
   license="MIT",
   long_description=long_description,
   author='WMI',
   author_email='sample@sample',
   packages=['src']
)