from redcap import Project, RedcapError
URL = 'https://redcap.partners.org/redcap/redcap_v10.0.30/API/project_api.php?pid=24843'
API_KEY = 'ExampleKey'
project = Project(URL, API_KEY)
