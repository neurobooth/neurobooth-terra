neurobooth-terra
================

.. image:: https://github.com/neurobooth/neurobooth-terra/actions/workflows/unit_tests.yml/badge.svg
   :target: https://github.com/neurobooth/neurobooth-terra/actions
   :alt: Github Actions

Dependencies
------------

* PostgreSQL (`https://www.postgresql.org/ <https://www.postgresql.org/>`_)
* psycopg2
* PyCap
* pandas (> 1.4.0)
* sshtunnel

Installation
------------

We recommend the `Anaconda Python distribution <https://www.anaconda.com/products/individual>`_.
To install ``neurobooth-terra``, first install pandas through conda using::

   $ conda install pandas

Then, simply do::

   $ pip install -e git+https://github.com/neurobooth/neurobooth-terra.git#egg=neurobooth_terra

and it will install ``neurobooth-terra`` along with the remaining dependencies which are not already installed.

To check if everything worked fine, you can do::

	$ python -c 'import neurobooth_terra'

and it should not give any error messages.

Bug reports
===========

Use the `github issue tracker <https://github.com/neurobooth/neurobooth-terra/issues>`_ to
report bugs.
