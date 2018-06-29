from setuptools import setup

setup(
    name="wapor",
    version='0.2',
    py_modules=['cli'],
    install_requires=[
        'Click',
    ],
    entry_points="""
        [console_scripts]
        wapor=cli.cli:main
    """,
)