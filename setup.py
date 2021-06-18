from setuptools import setup
from os import path
from setuptools import find_packages

BASE_DIR = path.dirname(path.abspath(__file__))
README_DIR = 'README.md'
with open(path.join(BASE_DIR, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().split()

setup(
    name='bools',
    version='0.2.0',
    description='常用工具库',
    author='bowaer',
    author_email='cb229435444@outlook.com',
    license='MIT',
    keywords=['tools', 'datetime', 'logger'],
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
