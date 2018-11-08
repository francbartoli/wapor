Authentication with Service account
===================================

There are two methods to getting authenticated into Google Earth Engine through
a Service Account identity.

Service account JSON file
-------------------------

Place your JSON file into the directory *gee_sa_files*. The file has to follow the structure as in the below example:

```JSON
{
  "type": "service_account",
  "project_id": "project-wapor",
  "private_key_id": "23f6bfb41ef3330dec15f3434f70a40c0967a0bc",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIDXTCCAkWgAwIBAgIJAJC1HiIAZAiIMA0GCSqGSIb3Df\nBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVx\naWRnaXRzIFB0eSBMdGQwHhcNMTExMjMxMDg1OTQ0WhcNMT\nA ....\nMANY LINES LIKE THAT ....\nJjyzfN746vaInA1KxYEeI1Rx5KXY8zIdj6a7hhphpj2E04\nC3Fayua4DRHyZOLmlvQ6tIChY0ClXXuefbmVSDeUHwc8Yu\nB7xxt8BVc69rLeHV15A0qyx77CLSj3tCx2IUXVqRs5mlSb\nvA==\n-----END PRIVATE KEY-----\n",
  "client_email": "project-wapor@project-wapor.iam.gserviceaccount.com",
  "client_id": "301283888490332229813",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/project-wapor%40project-wapor.iam.gserviceaccount.com"
}
```

Service account .env file
-------------------------

Place the relevant lines from the service account file into a key/value file in
the root folder of this repository:

```INI
PROJECT_ID=project-wapor
PRIVATE_KEY_ID=23f6bfb41ef3330dec15f3434f70a40c0967a0bc
PRIVATE_KEY=-----BEGIN PRIVATE KEY-----\nMIIDXTCCAkWgAwIBAgIJAJC1HiIAZAiIMA0GCSqGSIb3Df\nBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEwHwYDVx\naWRnaXRzIFB0eSBMdGQwHhcNMTExMjMxMDg1OTQ0WhcNMT\nA ....\nMANY LINES LIKE THAT ....\nJjyzfN746vaInA1KxYEeI1Rx5KXY8zIdj6a7hhphpj2E04\nC3Fayua4DRHyZOLmlvQ6tIChY0ClXXuefbmVSDeUHwc8Yu\nB7xxt8BVc69rLeHV15A0qyx77CLSj3tCx2IUXVqRs5mlSb\nvA==\n-----END PRIVATE KEY-----\n
CLIENT_EMAIL=project-wapor@project-wapor.iam.gserviceaccount.com
CLIENT_ID=301283888490332229813
RELATIVE_CLIENT_X509_CERT_URL=project-wapor%40project-wapor.iam.gserviceaccount.com
}
```

Once you are within the python virtual environment then those variables are available to the shell:

```shell
pipenv shell
env
```

WaPOR Calculations
===================

The FAO Water Productivity Open-Acces portal uses Remote sensing technologies to monitor and report on agriculture water
productivity over Africa and the Near East. WaPOR. For more information on how to use the library API Documentation
Annual data(AET-AGBP-T-WPnb-WPgb) 2010 – 2015 were previously calculated. Requests to recalculate these dataset will result in error.

Program Parameters.

usage: wpMain.py **timeframe** [-e {u,d,t}] [-i] [-a ['agbp', 'aet', 't_frac', 'wp_gb', 'wp_nb']] [-s] [-v] [-h]

- **bold** = mandatory

Arguments:
--------------   

    timeframe Calculate Water Productivity Annually for the chosen period and can be provided as a single year or two dates
              (e.g. 2015 or 2015-01-01 2015-01-31)

    -h 	show this help message and exit

    -x, --export, choices=['u', 'd', 't'],
                        help="Choose export to url(-u), drive (-d) or asset (-t)")

    -i, --map_id help="Generate map id for the chosen dataset"

    -s, --arealstat help="Zonal statistics chosen country and for the chosen dataset"

    -o, --output choices=['csv', 'json'],
                        help="Choose format fo the annual statistics csv(-o 'csv') or json (-o 'json')"

    -a, --aggregation choices=['agbp', 'aet', 't_frac', 'wp_gb', 'wp_nb'],
                       help="which dataset must be calculated between the chosen timeframe"

    -u --upload  help="Upload or update data in Google Earth Engine"

    -v, --verbose, help="Increase output verbosity"

    Only available to developers for testing purposes not available on server
    -------------------------------------------------------------------------
    -m,--map, choices=['agbp', 'aet', 't_frac', 'wp_gb', 'wp_nb']
                        help="Show calculated output overlaid on Google Map"

## Examples of Use
###Example 1
* Calculate Gross Biomass Water Productivity between 1st of January 2015 and 30th of January 2015
* Statistics calculated for Benin
* Generate map ID

    ##### wpMain.py 2015-1-1 2015-1-30 -a wp_gb -i -s "Benin"

###Example 2
* Calculate Gross Biomass Water Productivity for 2015 (already stored from 2010-2016)
  Will be possible to calculate new dataset only from 2017 onwards
* Generate map ID

    ##### wpMain.py 2015 -a wp_gb -i

###Example 3
* Calculate Gross Biomass Water Productivity between first and 31st of January 2012
  (Note the position of the parameter -i <b>BEFORE</b> -s)
* Generate map ID
* Calculate statistics for watershed named 'Rift Valley'

    ##### wpMain.py 2012-01-01 2012-01-31 -a wp_gb -i -s w 'Rift Valley'  

Water Productivity Data Management
==================================
Uploading or updating data in GEE. Valid credentials (a gmail account) must be provided.

###Example 1
Load all files contained in a directory
wpDataManagement.py  ../wapor_algorithms/image_test/

###Example 2
Load specific files
wpDataManagement.py ../wapor_algorithms/image_test/L2_EANE_PHE_09s1_e.tif
wpDataManagement.py ../wapor_algorithms/image_test/L1_RET_090406.tif
