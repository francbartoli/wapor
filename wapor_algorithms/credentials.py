import os
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

EE_ACCOUNT = 'fao-wapor@fao-wapor.iam.gserviceaccount.com'
EE_PRIVATE_KEY_FILE = os.path.join(__location__, 'gee_auth_files/WaterProductivity-60f6bfb41ef2.json')
GOOGLE_SERVICE_ACCOUNT_SCOPES = ['https://www.googleapis.com/auth/fusiontables',
                                 'https://www.googleapis.com/auth/earthengine']
