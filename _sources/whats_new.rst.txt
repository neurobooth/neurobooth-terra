What's new?
===========

Current
-------

Changelog
~~~~~~~~~

- Add new function ``redcap_service`` that's runs a loop and waits for
  ``update_interval`` seconds before going to next iteration.

Bug
~~~

- ``fetch_survey`` now replaces all NaN with ``None`` to be compatible with
  PostgreSQL.

API
~~~

- ``cols`` is a required column now in ``table.update_row`` and ``table.insert_rows``

- ``table.insert_rows`` now does nothing if conflict exists.

- ``table.query()`` now does not take a query as a string and returns the
   entire table as a dataframe.
