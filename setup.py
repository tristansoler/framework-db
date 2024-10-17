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
        where='src',
        exclude=['tests', 'tests.*']
    ),
    package_dir={'': 'src'},
    include_package_data=True
)