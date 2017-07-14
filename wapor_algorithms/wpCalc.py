#! /usr/bin/env python
# coding: utf-8
"""
    All calculation for Water Production biomass plus some additional features:

    1 - overlay map in a map viewer
    2 - generate a chart for each component used for calculating water productivity
    3 - generation of areal statistics (e.g. mean, max,etc...) for a country or river basin
    4 - generation of time-series for a specific collection of data stored in Google Earth Engine
    5 - export the calculated dataset in GDrive or GEE Asset

"""

import logging
import ee
from ee import mapclient
from geojson import FeatureCollection
import os

# Importing the credentials provided as JSON but ignored in code commits
import credentials as cr

class WaterProductivityCalc(object):

    #ee.Initialize()

    """Constructor for wpDataManagement"""
    EE_CREDENTIALS = ee.ServiceAccountCredentials(cr.EE_ACCOUNT, cr.EE_PRIVATE_KEY_FILE ,
                                                  cr.GOOGLE_SERVICE_ACCOUNT_SCOPES )
    ee.Initialize(EE_CREDENTIALS)

    _REGION = [[-25.0, -37.0], [60.0, -41.0], [58.0, 39.0], [-31.0, 38.0], [-25.0, -37.0]]
    _WSHEDS = ee.FeatureCollection('projects/fao-wapor/vectors/wapor_basins')
    _COUNTRIES = ee.FeatureCollection('projects/fao-wapor/vectors/wapor_countries')

    def __init__(self):
        pass

class L1WaterProductivity(WaterProductivityCalc):

    """
        Create Water Productivity raster file for annual and dekadal timeframes for Level 1 (Countries and Basins
    """

    _L1_RET_DAILY = ee.ImageCollection("projects/fao-wapor/L1_RET")
    _L1_PCP_DAILY = ee.ImageCollection("projects/fao-wapor/L1_PCP")
    _L1_NPP_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_NPP")
    _L1_AET_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_AET")
    _L1_TFRAC_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_TFRAC")

    # Land Cover for calculation to be changed in future
    _L1_LCC = ee.ImageCollection("projects/fao-wapor/L1_LCC_preliminary")

    # PRE-Calculated annual data stored as collections - New Calculation allowed form 2017 onwards
    _L1_AET_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_AET_Annual")
    _L1_AGBP_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_AGBP_Annual")
    _L1_T_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_T_Annual")
    _L1_WPgb_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_WPgb_Annual")
    _L1_WPnb_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_WPnb_Annual")

    def __init__(self):

        """ Constructor for date and dataset for WPgb"""
        super(L1WaterProductivity, self).__init__()

        self.L1_logger = logging.getLogger("wpWin.wpCalc")
        self.L1_AET_calc = self._L1_AET_DEKADAL
        self.L1_AGBP_calc = self._L1_NPP_DEKADAL

        image_for_scale_calculation = ee.Image("projects/fao-wapor/L1_TFRAC/L1_TFRAC_1003")
        # Get scale (in meters) information from first image
        self.scale_calc = image_for_scale_calculation.projection().nominalScale().getInfo()
        self.L1_logger.debug('Scale used for Level 1 calculation %f ' % self.scale_calc)

    def date_selection(self, **kwargs):
        '''
         Modify date for selecting datasets to be used for WPgm

        :param kwargs: date_start and date_end
        :return:
        '''

        self._date_start = str(kwargs.get('date_start'))
        self._date_end = str(kwargs.get('date_end'))

    def image_selection(self):

        """ Filter datasets selecting only images within the starting end ending date to be used for WPbm"""

        collection_agbp_filtered = L1WaterProductivity._L1_NPP_DEKADAL.filterDate(
            self._date_start,
            self._date_end)

        collection_aet_filtered = L1WaterProductivity._L1_AET_DEKADAL.filterDate(
            self._date_start,
            self._date_end)

        self.L1_AGBP_calc = collection_agbp_filtered
        self.L1_AET_calc = collection_aet_filtered

        agbp_num = collection_agbp_filtered.size().getInfo()
        aet_num = collection_aet_filtered.size().getInfo()

        self.L1_logger.debug("AGBP selected %d" % agbp_num)
        self.L1_logger.debug("AET selected %d" % aet_num)

        return self.L1_AGBP_calc, self.L1_AET_calc

    @property
    def multiply_npp(self, filtering_values):
        '''
        Sets the dataset to be used in conjunction with Actual Evapotranspiration for WPgb

        :param filtering_values: parameters used to filter the whole collection of images in this case only dates
        :return: AGBP filtered for WP calculation
        '''

        coll_npp_filtered = self._L1_NPP_DEKADAL.filterDate(
            self._date_start,
            self._date_end)
        coll_npp_multiplied = coll_npp_filtered.map(lambda npp_images: npp_images.multiply(filtering_values[0]))

        self.L1_AGBP_calc = coll_npp_multiplied

        return self.L1_AGBP_calc

    def agbp_aggregated(self):

        '''
        Aggregate above ground biomass productivity for annual calculation or water productivity
        :return:  aggregated data selected for WP calculation
        '''

        # the image.multiply(0.01444) multiplies all bands by 0.01444, including the days in dekad.
        # That is why the final WP values were so low...we should first multiply, then, in the same function, add the extra band
        def agbp_multiplication(image):
            img_multi = image.multiply(0.01444).addBands(image.metadata('days_in_dk'))
            return img_multi
        agbp_npp_multiplied = self.L1_AGBP_calc.map(agbp_multiplication)

        # Get AGBP value, divide by 10 (as per FRAME spec) to get daily value, and multiply by number of days in dekad
        # we don't need to divide by 10 now: it was previously valid on sample data, and we don't use AGBP as input anyway.
        def npp_add_dk(image):
            mmdk = image.select('b1').multiply(image.select('days_in_dk'))
            return mmdk
        npp_with_dekad = agbp_npp_multiplied.map(npp_add_dk)

        aggregated_agbp = npp_with_dekad.reduce(ee.Reducer.sum())

        return aggregated_agbp

    def aet_aggregated(self):

        """Aggregate actual evapotranspiration for annual calculation or water productivity"""

        coll_aet_sorted = self.L1_AET_calc.sort('system:time_start', True)
        aggregated_aet = coll_aet_sorted.reduce(ee.Reducer.sum())

        return aggregated_aet

    def transpiration(self):

        """Aggregate transpiration using actual evapotranspiration,above ground biomass productivity and t_fraction"""

        # This is calculated at class level for WPgb, WPnb
        # collAETFiltered = _L1_AET_DEKADAL.filterDate(start, end).sort('system:time_start', True)
        L1_TFRAC_calc = L1WaterProductivity._L1_TFRAC_DEKADAL.filterDate(self._date_start, self._date_end).sort('system:time_start', True)


        # JOINING TWO Collections - Start
        Join = ee.Join.inner()
        FilterOnStartTime = ee.Filter.equals(
            leftField='system:time_start',
            rightField='system:time_start'
        )

        TFRAC_AET_collection_Join = ee.ImageCollection(Join.apply(self.L1_AET_calc,
                                                                  L1_TFRAC_calc,
                                                                  FilterOnStartTime))
        self.L1_logger.debug("Joined collections %s", TFRAC_AET_collection_Join.getInfo())
        # JOINING TWO Collections - End

        def Tfrac_AETdk(image):
            image_aet = ee.Image(image.get("primary"))
            image_tfrac = ee.Image(image.get("secondary"))
            t_a = ((image_aet.select('b1').multiply(image_tfrac.select('b1').divide(100)))
                   .multiply(0.1)).multiply(ee.Number(image_aet.get('days_in_dk'))).float()
            return ee.Image(t_a)
        T_annual = TFRAC_AET_collection_Join.map(Tfrac_AETdk)

        SUM_TFRAC_annual = T_annual.reduce(ee.Reducer.sum()).toFloat()
        return SUM_TFRAC_annual

    def water_productivity_gross_biomass(self):

        """wp_gross_biomass calculation returns all intermediate results besides the final wp_gross_biomass"""

        # Multiplied for generating AGBP from NPP using the costant 0.144  CHANGED 0.0144 for Release 1
        npp_multiplied = self.L1_AGBP_calc.map(lambda list_agbp: list_agbp.multiply(0.01444).addBands(list_agbp.metadata('days_in_dk')))

        # .multiply(10); the multiplier will need to be
        # applied on net FRAME delivery, not on sample dataset
        agbp = npp_multiplied.sum()

        # add image property (days in dekad) as band
        eta_dekad_added = self.L1_AET_calc.map(lambda imm_eta2: imm_eta2.addBands(
                                                 imm_eta2.metadata(
                                                 'days_in_dk')))

        # get ET value, divide by 10 (as per FRAME spec) to get daily
        # value, and  multiply by number of days in dekad summed annuallyS
        aet_dekad_multiplied = eta_dekad_added.map(lambda imm_eta3: imm_eta3.select('b1')
                                                   .divide(10)
                                                   .multiply(imm_eta3.select('days_in_dk'))).sum()

        # scale ETsum from mm/m² to m³/ha for WP calculation purposes
        eta = aet_dekad_multiplied.multiply(10)

        # calculate biomass water productivity and add to map
        wp_gross_biomass = agbp.divide(eta)

        return agbp, eta, wp_gross_biomass

    @staticmethod
    def water_productivity_net_biomass_pre_calculated_annual_values(year):
        '''
        wp_net_biomass calculation simplified method using pre-calculated annual value fot AGBP and T

        :param year: year the wp will be calcualted
        :return:
        '''

        agbp_y = ee.Image("projects/fao-wapor/AGBP_Annual/AGBP-" + str(year))
        t_y = ee.Image("projects/fao-wapor/T_Annual/T_Annual-" + str(year))

        t_masked = t_y.select('b1_sum').gte(100)

        # /********* ORIGINAL MATH ***********/
        # Or, as you will already have calculated AET annual and AGBP annual:
        # AGBPy/(AETy*10) where *10 is to convert mm into m³/ha
        wp_nb_precalc = agbp_y.divide(t_masked.multiply(10))

        return wp_nb_precalc

    def water_productivity_net_biomass_dates(self):

        """wp_net_biomass calculation returns all intermediate results besides the final wp_gross_biomass."""

        # Or, as you will already have calculated AET annual and AGBP annual:
        # AGBPy/(AETy*10) where *10 is to convert mm into m³/ha
        # var WPnb = AGBPy.divide(Ty.multiply(10));
        def T_moreThan100(image):
            return image.mask(image.select('b1_sum').gte(100))
        t_year_coll_mt100 = self._L1_T_ANNUAL.map(T_moreThan100)

        # agbp_num = collection_agbp_filtered.size ().getInfo ()
        # self.L1_logger.debug ( "AGBP selected %d" % agbp_num )

        # # JOINING and Merging Final Method
        # JOINING agbp and t annual - Start
        Join = ee.Join.inner()
        FilterOnStartTime = ee.Filter.equals(
            leftField='system:time_start',
            rightField='system:time_start'
        )
        AGBP_T_collection_Join = ee.ImageCollection(
            # TODO: CAMBIARE _L1_AGBP_ANNUAL
            Join.apply(self._L1_AGBP_ANNUAL,
                       t_year_coll_mt100,
                       FilterOnStartTime))
        self.L1_logger.debug( AGBP_T_collection_Join.size ().getInfo ()) #"Joined collections",
        # JOINING TWO Collections - End

        # MERGING JOINED agbp and t annual - Start
        def MergeBands(element):
            return ee.Image.cat(element.get('primary'), element.get('secondary'))

        agbp_t_coll_merged = AGBP_T_collection_Join.map(MergeBands)
        self.L1_logger.debug("Merged Joined collections", agbp_t_coll_merged)
        # MERGING JOINED TWO Collections - End

        # /********* WPnb ****************/
        def AGBP_T(image):
            wp_nb = image.select('b1_sum').divide(image.select('b1_sum_1').multiply(10))
            return ee.Image(wp_nb)

        WPnb_coll = agbp_t_coll_merged.map(AGBP_T)

        #self.L1_logger.debug("WPnb_coll" % WPnb_coll)

    def map_id_getter(self,**outputs_id):

        """Generate a map id and a token for the calculated WPbm raster file"""

        map_ids = {}
        for key, val in outputs_id.iteritems():
            map_id = val.getMapId()
            map_ids[key] = {}
            map_ids[key]['mapid'] = map_id['mapid']
            map_ids[key]['token'] = map_id['token']
            map_ids[key]['image'] = map_id['image'].getInfo()

        return map_ids

    @staticmethod
    def image_visualization(raster_name, raster_plot):

        """Output the calculated raster using a map vizualizer """

        VisPar_AGBPy = {"opacity": 0.85, "bands": "b1", "min": 0, "max": 180,
                       "palette": "f4ffd9,c8ef7e,87b332,566e1b",
                       "region": WaterProductivityCalc._REGION}

        VisPar_ETay = {"opacity": 1, "bands": "b1", "min": 0, "max": 2000,
                      "palette": "d4ffc6,beffed,79c1ff,3e539f",
                      "region": WaterProductivityCalc._REGION}

        VisPar_TFRAC = {"opacity": 1,  "bands": "b1_sum",
                        "max": 800, "palette": "fffcdb,b4ffa6,3eba70,195766",
                        "region": WaterProductivityCalc._REGION}

        VisPar_WPgb = {"opacity": 0.85, "bands": "b1", "max": 1.2,
                      "palette": "bc170f,e97a1a,fff83a,9bff40,5cb326",
                      "region": WaterProductivityCalc._REGION}

        if raster_name == 'aet':
            legend = VisPar_ETay
        elif raster_name == 'agbp':
            legend = VisPar_AGBPy
        elif raster_name == 't_frac':
            legend = VisPar_TFRAC
        elif raster_name == 'wp_gb':
            legend = VisPar_WPgb
        elif raster_name == 'wp_nb':
            legend = VisPar_WPgb

        mapclient.addToMap(raster_plot, legend, raster_name)
        mapclient.centerMap(17.75, 10.14, 4)

    def generate_areal_stats(self, areal_option, query_object, wbpm_calc):

        """Calculates several statistics for the Water Productivity calculated raster for a chosen dataset / name"""
        num_areas = 0
        if areal_option == 'c':
            try:
                calculation_area = WaterProductivityCalc._COUNTRIES.filter(ee.Filter.eq('iso3', query_object))
                num_areas = calculation_area.size().getInfo()
                cut_poly = calculation_area.geometry()
            except:
                error = Exception('no country')
        elif areal_option == 'w':
            try:
                calculation_area = WaterProductivityCalc._WSHEDS.filter( ee.Filter.eq('maj_name',query_object))
                num_areas = calculation_area.size().getInfo()
                cut_poly = calculation_area.geometry()
            except:
                error = Exception('no watershed')
        elif areal_option == 'g':
            try:
                data = FeatureCollection ( query_object )
                cut_poly = data['features']['features'][0]['geometry']
                if len(cut_poly)>0:
                    num_areas = 1
            except:
                error = Exception('Empty user defined area')

        if num_areas > 0:
            means = wbpm_calc.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=cut_poly,
                scale=self.scale_calc,
                maxPixels=1e9
            )
            mean = means.getInfo()

            reducers_min_max_sum = ee.Reducer.minMax().combine(
                reducer2=ee.Reducer.sum(),
                sharedInputs=True
            )

            # Use the combined reducer to get the min max and SD of the image.
            stats = wbpm_calc.reduceRegion(
                reducer=reducers_min_max_sum,
                bestEffort=True,
                geometry=cut_poly,
                scale=self.scale_calc
            )
            min_max_sum = stats.getInfo()

            statistics_for_chosen_area = {}
            statistics_for_chosen_area['response'] = {}
            statistics_for_chosen_area['response']['name'] = query_object
            if areal_option == 'c':
                statistics_for_chosen_area['response']['iso3'] = calculation_area.getInfo()['features'][0]['properties']['iso3']
                statistics_for_chosen_area['response']['gaul_code'] = calculation_area.getInfo()['features'][0]['properties']['gaul_code']
            elif areal_option == 'w':
                statistics_for_chosen_area['response']['wshed_code'] = int(calculation_area.getInfo()['features'][0]['properties']['maj_bas'])
                statistics_for_chosen_area['response']['wapor_code'] = calculation_area.getInfo()['features'][0]['properties']['wapor_bas']
            statistics_for_chosen_area['response']['stats'] = {}
            statistics_for_chosen_area['response']['stats']['min'] = min_max_sum.pop('b1_min')
            statistics_for_chosen_area['response']['stats']['sum'] = min_max_sum.pop('b1_sum')
            statistics_for_chosen_area['response']['stats']['max'] = min_max_sum.pop('b1_max')
            statistics_for_chosen_area['response']['stats']['mean'] = mean.pop('b1')
            return statistics_for_chosen_area
        else:
            self.L1_logger.error("Error: %s named %s" % (error, query_object))
            return error

class L2WaterProductivity(WaterProductivityCalc):

    """
        Create Water Productivity raster file for annual and dekadal timeframes for Level 2
    """
    _L2_EANE_AET_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_EANE_AET")
    _L2_EANE_PHE_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_EANE_PHE")
    _L2_WANE_AET_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_WANE_AET")
    _L2_WANE_NPP_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_WANE_NPP")
    _L2_EANE_TFRAC_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_EANE_TFRAC")

    def __init__(self):

        """ Calculations for Level 2"""
        super(L2WaterProductivity, self).__init__()
