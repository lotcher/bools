from setuptools import setup, find_packages
from os import path

BASE_DIR = path.dirname(path.abspath(__file__))
README_DIR = 'README.md'
with open(path.join(BASE_DIR, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().split()

setup(
    name='bools',
    version='0.4.2.1',
    description='Collection of common tools in Python',
    author='bowaer',
    author_email='cb229435444@outlook.com',
    license='MIT',
    keywords=['tools', 'datetime', 'logger', 'functools', 'elasticsearch', 'influxdb'],
    url='https://github.com/lotcher/bools',
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        'Programming Language :: Python :: 3.6',
    ],
    long_description=open(README_DIR, encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    data_files=[README_DIR]
)
