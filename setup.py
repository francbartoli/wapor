from setuptools import setup

setup(
    name="wapor",
    version='0.1',
    py_modules=['cli'],
    install_requires=[
        'Click',
    ],
    entry_points="""
        [console_scripts]
        wapor=cli:cli
    """,
)