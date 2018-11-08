=========
wapor
=========


.. image:: https://img.shields.io/pypi/v/wapor.svg
   :target: https://pypi.python.org/pypi/wapor

.. image:: https://img.shields.io/travis/francbartoli/wapor.svg
   :target: https://travis-ci.org/francbartoli/wapor

.. image:: https://readthedocs.org/projects/gee-pheno/badge/?version=latest
   :target: https://gee-pheno.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status


.. image:: https://pyup.io/repos/github/francbartoli/wapor/shield.svg
   :target: https://pyup.io/repos/github/francbartoli/wapor/
   :alt: Updates



WAPOR


* Free software: GPLv3 license
* Documentation: https://wapor.readthedocs.io.


Features
--------

* Calculate the following products:
    * AETI
    * AGBP
    * GBWP
    * NBWP

Install
-------

Using pip
^^^^^^^^^

Assuming you have already created a python virtual environment, activate it and
then install the tool::

    pip install wapor --process-dependency-links

Using pipenv
^^^^^^^^^^^^

Choose a directory where you want to spawne the python virtual environment and
install the tool with pipenv, then from there::

    pipenv shell --two

You will be led to a new shell where you can execute::

    pipenv install wapor

Any warning can be safely ignored and finally you can exit from this shell::

    exit

The tool is ready to be configured for the authentication with your
Google Earth Engine service account.

Usage
-----

Pip
^^^

In case you have installed it systemwide you don't need to activate any
virtual environment otherwise you have to as preliminary step::

    wapor --help

Pipenv
^^^^^^

If you are using pipenv the only requirement to run the wapor command is
to go to the directory where you have executed the installation steps and run::

    pipenv run wapor --help

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
