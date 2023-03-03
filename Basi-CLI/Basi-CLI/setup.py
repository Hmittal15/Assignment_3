from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='Basi_CLI',
    version='1.4',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'Basi-CLI=Basi_CLI.cli:app',
        ],
    },
    install_requires=[
        "numpy",
        "pandas",
        "typer",
        "requests",
        "click",
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    author='Lakshman Raaj Senthil Nathan',
    author_email='lakshmanraajs@gmail.com',
)