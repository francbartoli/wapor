#! /usr/bin/env python
"""
    Main class for activating different calculations available in wpCalc.py via argparse
"""
import argparse
import logging
import sys
import os

from wpCalc import L1WaterProductivity
from wpDataManagement import DataManagement as dm

def setup(args=None, parser=None):

    parser = argparse.ArgumentParser(description='Water Productivity using Google Earth Engine')

    parser.add_argument("timeframe",
                        nargs="*",
                        help="Calculate Water Productivity Annually for the chosen period"
                        )

    parser.add_argument('-x', '--export', choices=['drive', 'asset'],
                        help="Choose export to Google Drive or GEE FAO asset space.")

    parser.add_argument('-i', '--map_id',
                        help="Generate map id for generating tiles",
                        action="store_true")

    parser.add_argument('-s', '--arealstat',
                        choices=['c','w','g'],
                        nargs=argparse.REMAINDER,
                        help="Zonal statistics form a WaterProductivity generated in GEE "
                             "for the chosen Country/Watershed or User Defined Area")

    parser.add_argument('-o', '--output',
                        choices=['csv', 'json'],
                        help="Choose format fo the annual statistics csv(-o 'csv') or json (-o 'json')")

    parser.add_argument("-a",
                        "--aggregation",
                        choices=['agbp', 'aet', 't_frac', 'wp_gb', 'wp_nb'],
                        help="Aggregate dekadal data at annual level"
                        )

    parser.add_argument("-m",
                        "--map",
                        choices=['agbp', 'aet', 't_frac', 'wp_gb', 'wp_nb'],
                        help="Show calculated output overlaid on Google Map"
                        )

    parser.add_argument("-v", "--verbose",
                        help="Increase output verbosity",
                        action="store_true")

    return parser

def run(results):

    main_logger = logging.getLogger("wpWin")
    main_logger.setLevel(level=logging.DEBUG)

    formatter = logging.Formatter("%(levelname) -4s %(asctime)s %(module)s:%(lineno)s %(funcName)s %(message)s")

    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    fh = logging.FileHandler(os.path.join(__location__, 'log_files/wapor.log'))
    fh.setLevel(logging.ERROR)
    fh.setFormatter(formatter)
    main_logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    main_logger.addHandler(ch)

    # def methods(**kwargs):
    #     print kwargs
    # methods(**vars(results))

    analysis_level_1 = L1WaterProductivity()

    if len(results.timeframe) == 1:
        input_year = str(results.timeframe[0])
        date_start = input_year + '-01-01'
        date_end = input_year + '-12-31'
    elif len(results.timeframe) == 2:
        date_start = results.timeframe[0]
        date_end = results.timeframe[1]
    else:
        print("date error")
        pass

    selection_params = {'date_start': date_start, 'date_end': date_end}
    main_logger.debug(selection_params)
    analysis_level_1.date_selection(**selection_params)
    analysis_level_1.image_selection()

    if results.aggregation:

        if isinstance(results.aggregation, list):
            selection_aggregation = results.aggregation[0]
        else:
            selection_aggregation = results.aggregation

        main_logger.debug("Working on %s " % selection_aggregation)

        if selection_aggregation == 'aet':
            eta = analysis_level_1.aet_aggregated()
        if selection_aggregation == 'agbp':
            agbp = analysis_level_1.agbp_aggregated()
        if selection_aggregation == 'wp_gb':
            agbp, eta, wp_gb = analysis_level_1.water_productivity_gross_biomass()
        if selection_aggregation == 'wp_nb':
            #This is the water productivity NET calcualted between any two dates
            #wp_nb = analysis_level_1.water_productivity_net_biomass_dates()

            # This is the water productivity NET calculated on a yearly basis which has been implemented
            # in code editor in Javascript for the first release of the project
            wp_nb = analysis_level_1.water_productivity_net_biomass_pre_calculated_annual_values(input_year)

        if selection_aggregation == 't_frac':
            eta = analysis_level_1.aet_aggregated()
            t_frac = analysis_level_1.transpiration()

    if results.map:

        if results.map == 'aet':
            analysis_level_1.image_visualization(results.map, eta)
        if results.map == 'agbp':
            analysis_level_1.image_visualization(results.map, agbp)
        if results.map == 'wp_gb':
            analysis_level_1.image_visualization(results.map, wp_gb)
        if results.map == 't_frac':
            analysis_level_1.image_visualization(results.map, t_frac)

    if results.arealstat is not None:

        tasks = dm.get_tasks()
        num_task_running,name_tasks_running = dm.running_tasks(tasks)

        if len ( num_task_running ) == 0:
            main_logger.debug("No files are being uploaded at the moment")
        else:
            main_logger.debug("Tasks running at the moment %d" % len ( num_task_running ))
            pass

        if isinstance(results.arealstat, list) :
            try:
                area_stats = analysis_level_1.generate_areal_stats(results.arealstat[0], results.arealstat[1], wp_gb)
                main_logger.debug("RESPONSE=%s" % area_stats)
            except Exception as e:
                if isinstance(e, UnboundLocalError):
                    main_logger.debug("WP_GP aggregation Error")
                    main_logger.error(e)
                elif results.arealstat[0] == 'c':
                    main_logger.debug("Country Error")
                    main_logger.error("No country named {}".format(results.arealstat[1]))
                elif results.arealstat[0] == 'w':
                    main_logger.debug("Watershed Error")
                    main_logger.error("No watershed named {}".format(results.arealstat[1]))
                elif results.arealstat[0] == 'g':
                    main_logger.debug("User Defined Area format Error")
                    main_logger.error("Invalid GeoJson {} to parse".format(results.arealstat[1]))
        else:
            main_logger.debug("Invalid arealstat arguments format")
    else:
        pass

    if results.map_id:

        if results.aggregation is None:
            main_logger.error ( "RESULT=%s" % "Water Productivity Gross Biomass has not been calcualted" )
        else:
            map_ids = {'agbp': agbp, 'eta': eta, 'wp_gross': wp_gb}
            main_logger.debug("RESULT=%s" % analysis_level_1.map_id_getter(**map_ids))

    if results.export:

        if results.aggregation is None :
            main_logger.error ("Only an aggregated dataset can be exported choose -a and the required aggregation level" )
        else:
            main_logger.info ( "Exporting %s to %s" % (results.aggregation, results.export) )
            if results.aggregation == 'wp_gb':
                dm.image_export(results.export, wp_gb, "wp_gb", date_start, date_end)
            elif results.aggregation == 'wp_nb':
                dm.image_export ( results.export,wp_nb)
            else:
                main_logger.error ("That dataset cannot be exported")
                pass

    args = {k: v for k, v in vars(results).items() if v is not None}
    main_logger.debug("Final Check %s" % args)

    # analysis_level_1.image_export(results.export, wp_gb)

if __name__ == '__main__':

    # Updated upstream
    results = setup().parse_args()
    run(results)