import ee
from wapor_algorithms import credentials as cr

"""Constructor for wpDataManagement"""
EE_CREDENTIALS = ee.ServiceAccountCredentials ( cr.EE_ACCOUNT , cr.EE_PRIVATE_KEY_FILE ,
                                                cr.GOOGLE_SERVICE_ACCOUNT_SCOPES )
print(EE_CREDENTIALS)
ee.Initialize ( EE_CREDENTIALS )

# from oauth2client.client import flow_from_clientsecrets
# flow = flow_from_clientsecrets( 'gee_login/client_secret_103283993465834459813.json',
#                                  scope = cr.GOOGLE_SERVICE_ACCOUNT_SCOPES) #,
#                                  #redirect_uri='http://www.example.com/oauth2callback')
# You don't want to load flow_from_clientsecrets for a service account. In
# fact, you don't need a flow at all for a service account.
# http://google-api-python-client.googlecode.com/hg/docs/epy/oauth2client.client.SignedJwtAssertionCredentials-class.html

SCOPES = [cr.GOOGLE_SERVICE_ACCOUNT_SCOPES ]
print(SCOPES)

import os
print(os.path.dirname(__file__))

import utils
print(utils.TASK_FINISHED_STATES)
print(utils.DEFAULT_EE_CONFIG_FILE_RELATIVE)