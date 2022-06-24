:orphan:

.. _api_documentation:

=================
API Documentation
=================

Tables (:py:mod:`neurobooth_terra`):
------------------------------------

.. currentmodule:: neurobooth_terra

.. autosummary::
   :toctree: generated/

   Table
   create_table
   drop_table
   list_tables
   query

Redcap (:py:mod:`neurobooth_terra.redcap`)
------------------------------------------
These are general redcap functions.

.. currentmodule:: neurobooth_terra.redcap

.. autosummary::
   :toctree: generated/

   fetch_survey
   dataframe_to_tuple
   rename_subject_ids

Data dictionary
===============
These are functions related to processing data dictionary

.. currentmodule:: neurobooth_terra.redcap

.. autosummary::
   :toctree: generated/

   extract_field_annotation
   map_dtypes
   get_response_array
   get_tables_structure
   subselect_table_structure

Data flow
---------

.. currentmodule:: neurobooth_terra.dataflow

.. autosummary::
   :toctree: generated/

   write_files
   copy_files
   delete_files
