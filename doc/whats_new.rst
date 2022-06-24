What's new?
===========

Current
-------

Changelog
~~~~~~~~~

- Add new function ``redcap_service`` that's runs a loop and waits for
  ``update_interval`` seconds before going to next iteration.

- ``table.insert_rows`` now has an argument ``on_conflict`` to determine
  what action to take when there is a conflict.

- ``table.query`` argument ``column_names`` is now called ``include_columns``.

- New function ``query`` to make arbitrarily complex queries.

Bug
~~~

- ``fetch_survey`` now replaces all NaN with ``None`` to be compatible with
  PostgreSQL.

API
~~~

- ``cols`` is a required column now in ``table.update_row`` and ``table.insert_rows``

- ``table.query()`` now does not take a query as a string and returns the
   entire table as a dataframe.

- ``table.query()`` now accepts ``where`` as an argument for filtering rows.
