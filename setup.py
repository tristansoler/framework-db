from setuptools import setup, find_packages

setup(
    name='data_framework',
    version='1',
    description='Sample',
    license="MIT",
    author='WMI',
    author_email='sample@sample',
    python_requires='>=3.9',
    packages=find_packages(
        include=['src'],
        exclude=['tests']
    )
)