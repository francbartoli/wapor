
Welcome to WaPOR’s documentation!
===================

The FAO Water Productivity Open-Acces portal uses Remote sensing technologies to monitor and report on agriculture water 
productivity over Africa and the Near East. WaPOR. For more information on how to use the library API Documentation
Annual data(AET-AGBP-T-WPnb-WPgb) 2010 – 2015 were previously calculated. Requests to recalculate these dataset will result in error.


Program Parameters.

usage: wpMain.py **timeframe** [-e {u,d,t}] [-i] [-a ['agbp', 'aet', 't_frac', 'wp_gb', 'wp_nb']] [-s] [-v] [-h]
    
- **bold** = mandatory

Arguments:
--------------   
     
    timeframe Calculate Water Productivity Annually for the chosen period 
    
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

## Simple Use
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

###Example 4
Uploading or updating data in GEE. Valid credentials (a gmail account) must be provided.

* Using wpDataManagement. Preferred method. </br>
    ##### wpDataManagement.py ../snippets/real_image/L1_AET_1525.tif

* Using wpMain.py . Passing 0 water productivity will not be calculated.</br>
    ##### wpMain.py 0 -u '../snippets/files_test/L1_AET_0910.tif'

