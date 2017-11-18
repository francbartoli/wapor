import logging
import os
import uuid

import scandir
import simplejson as sjson
from ee import ServiceAccountCredentials
from oauth2client.service_account import ServiceAccountCredentials as SACreds

__location__ = os.path.realpath(os.path.join(
    os.getcwd(), os.path.dirname(__file__)))

# EE_ACCOUNT = 'fao-wapor@fao-wapor.iam.gserviceaccount.com'
GOOGLE_SERVICE_ACCOUNT_SCOPES = [
    'https://www.googleapis.com/auth/fusiontables',
    'https://www.googleapis.com/auth/earthengine'
]

EE_PRIVATE_SERVICEACCOUNT_JSON_TEMPLATE = 'ServiceAccount-template.json'

EE_PRIVATE_SERVICEACCOUNT_DIR = os.path.join(
    __location__,
    'gee_sa_files'
)

EE_PRIVATE_SERVICEACCOUNT_JSON_FILE = os.path.join(
    EE_PRIVATE_SERVICEACCOUNT_DIR,
    EE_PRIVATE_SERVICEACCOUNT_JSON_TEMPLATE
)

try:
    if os.path.exists(
        EE_PRIVATE_SERVICEACCOUNT_JSON_FILE
    ):
        if ((
            os.environ.get('PROJECT_ID') is not None
        ) and (
            os.environ.get('PRIVATE_KEY_ID') is not None
        ) and (
            os.environ.get('PRIVATE_KEY') is not None
        ) and (
            os.environ.get('CLIENT_EMAIL') is not None
        ) and (
            os.environ.get('CLIENT_ID') is not None
        ) and (
            os.environ.get('RELATIVE_CLIENT_X509_CERT_URL') is not None
        )
        ):
            with open(os.path.join(
                EE_PRIVATE_SERVICEACCOUNT_DIR,
                str(uuid.uuid1()) + '.json'
            ), 'w') as outfile:
                EE_ACCOUNT = os.environ['CLIENT_EMAIL']
                data = {}
                with open(EE_PRIVATE_SERVICEACCOUNT_JSON_FILE) as json_ee_sa:
                    keys = sjson.load(json_ee_sa)
                    keys['project_id'] = os.environ['PROJECT_ID']
                    keys['private_key_id'] = os.environ['PRIVATE_KEY_ID']
                    keys['private_key'] = os.environ['PRIVATE_KEY'].replace(
                        "\\\\n", "\n"
                    )
                    # hack to fix the presence of special character (\n)
                    # not well managed from .env is to wrap with double quotes
                    keys['client_email'] = os.environ['CLIENT_EMAIL']
                    keys['client_id'] = os.environ['CLIENT_ID']
                    keys['client_x509_cert_url'] = keys[
                        'client_x509_cert_url'
                    ].replace("RELATIVE_CLIENT_X509_CERT_URL",
                              os.environ['RELATIVE_CLIENT_X509_CERT_URL'])
                    data = keys
                sjson.dump(data, outfile)
            EE_PRIVATE_KEY_FILE = outfile.name
            _sacreds = ServiceAccountCredentials(
                EE_ACCOUNT,
                EE_PRIVATE_KEY_FILE,
                GOOGLE_SERVICE_ACCOUNT_SCOPES
            )
        else:
            for fn in [elem.path for elem in scandir.scandir(
                EE_PRIVATE_SERVICEACCOUNT_DIR
            ) if (
                elem.is_file() and elem.path.endswith(".json") and
                elem.path not in EE_PRIVATE_SERVICEACCOUNT_JSON_FILE
            )
            ]:
                with open(fn) as json_ee_safile:
                    keys = sjson.load(json_ee_safile)
                    _sacreds = ServiceAccountCredentials(
                        keys['client_email'],
                        fn,
                        GOOGLE_SERVICE_ACCOUNT_SCOPES
                    )
                    if not _sacreds.invalid:
                        logging.info(
                            "The Service Account file {} \
is gone to be used".format(
                                str(fn))
                        )
                        break
    else:
        _sacreds = None
        logging.ERROR("Service Account template has not been configured!")
        raise IOError("Starting path not found")
except Exception as e:
    raise e
else:
    pass
finally:
    if isinstance(_sacreds, SACreds):
        EE_CREDENTIALS = _sacreds
    else:
        logging.ERROR("Something failed, you are not authenticated.")
        EE_CREDENTIALS = None
