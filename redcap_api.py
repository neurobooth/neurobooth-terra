import os
from redcap import Project, RedcapError

# XXX: consider using requests directly?

URL = 'https://redcap.partners.org/redcap/api/'
API_KEY = os.environ.get('NEUROBOOTH_REDCAP_TOKEN')
metadata_fields = ['field_label', 'form_name', 'section_header',
                   'field_type', 'field_label', 'select_choices_or_calculations',
                   'required_field']

if API_KEY is None:
    raise ValueError('Please define the environment variable NEUROBOOTH_REDCAP_TOKEN first')

project = Project(URL, API_KEY, lazy=True)
print('Fetching metadata ...')
metadata = project.export_metadata(format='df')
metadata = metadata[metadata_fields]
print('[Done]')
