What's new?
===========

Current
-------

Changelog
~~~~~~~~~

Bug
~~~

API
~~~

- `cols` is a required column now in `table.update_row` and `table.insert_rows`

- `table.insert_rows` now does nothing if conflict exists.

- `table.query()` now does not take a query as a string and returns the
   entire table as a dataframe.
