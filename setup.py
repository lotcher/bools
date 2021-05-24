from setuptools import setup
from os import path

BASE_DIR = path.dirname(path.abspath(__file__))
with open(path.join(BASE_DIR, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().split()

setup(
    name='bools',
    version='0.0.1',
    description='常用工具库',
    author='bowaer',
    author_email='cb229435444@outlook.com',
    license='MIT',
    keywords=['tools'],
    url='https://github.com/lotcher/bools',
    packages=['bools'],
    install_requires=requirements,
    classifiers=[
        'Programming Language :: Python :: 3',
    ],
)
