===============
Getting started
===============

------------------------------------------------
Install dependencies & start the service locally
------------------------------------------------

1. Enter the project folder:

.. code-block:: console

    $ cd pilotecmwf_pp_starter

2. Install packages

.. code-block:: console

    $ poetry install


3. Run the job

.. code-block:: console

    $ poetry run python -m pilotecmwf_pp_starter

-------------------------------
Run the tests and quality tools
-------------------------------

1. Run tests

.. code-block:: console

    $ poetry run pytest

2. Run pylint

.. code-block:: console

    $ poetry run pylint pilotecmwf_pp_starter


3. Run mypy

.. code-block:: console

    $ poetry run mypy pilotecmwf_pp_starter


----------------------
Generate documentation
----------------------

.. code-block:: console

    $ poetry run sphinx-build doc doc/_build

Then open the index.html file generated in *pilotecmwf_pp_starter/build/sphinx/html*


.. HINT::
   All **poetry run** prefixes in the commands can be avoided if running them within the poetry shell
