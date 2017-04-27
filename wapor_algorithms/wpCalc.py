#! /usr/bin/env python
# coding: utf-8
"""
    All calculation for Water Production biomass plus some additional features:

    1 - overlay map in a map viewer
    2 - generate a chart for each componenet used for calculating sater productivity
    3 - generation of areal statistics (e.g. mean, max,etc...) for a country or river basin
    4 - generation of timeseries for a specific collection of data stored in Google Earth Engine
    5 - export the calculated dataset in GDrive, GEE Asset, geoserver, etc...

"""

import ee
import time
import ee.mapclient
import matplotlib.pyplot as plt
import sys
import os
import glob
import datetime
import seaborn
import pandas as pd
import logging

from osgeo import ogr

class WaterProductivityCalc(object):

    def __init__(self):
        _REGION = [[-25.0, -37.0], [60.0, -41.0], [58.0, 39.0], [-31.0, 38.0], [-25.0, -37.0]]
        _COUNTRIES = ee.FeatureCollection('ft:1ZDEMjtnWm_smu7l_z3fx91BbxyCRzP2A3cEMrEiP')
        _WSHEDS = ee.FeatureCollection('ft:1IXfrLpTHX4dtdj1LcNXjJADBB-d93rkdJ9acSEWK')

class L1WaterProductivity(WaterProductivityCalc):

    """
        Create Water Productivity raster file for annual and dekadal timeframes for Level 1 (Countries and Basins
    """

    def __init__(self):

        ee.Initialize()

        self.L1_logger = logging.getLogger("wpWin.wpCalc")

        self._L1_NPP_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_NPP")
        self._L1_AET_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_AET")
        self._L1_TFRAC_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_TFRAC")
        self._L1_RET_DAILY = ee.ImageCollection("projects/fao-wapor/L1_RET")
        self._L1_PCP_DAILY = ee.ImageCollection("projects/fao-wapor/L1_PCP")

        self.L1_AET_calc = self._L1_AET_DEKADAL
        self.L1_AGBP_calc = self._L1_NPP_DEKADAL

        self.VisPar_AGBPy = {"opacity": 0.85, "bands": "b1", "min": 0, "max": 850,
                             "palette": "f4ffd9,c8ef7e,87b332,566e1b", "region": WaterProductivityCalc._REGION}
        

        self.VisPar_ETay = {"opacity": 1, "bands": "b1", "min": 0, "max": 2000,
                            "palette": "d4ffc6,beffed,79c1ff,3e539f", "region": WaterProductivityCalc._REGION}

        self.VisPar_WPgb = {"opacity": 0.85, "bands": "b1", "min": 0, "max": 1.2,
                            "palette": "bc170f,e97a1a,fff83a,9bff40,5cb326", "region": WaterProductivityCalc._REGION}

    @property
    def image_selection(self):

        """ Returns datasets to be used for WPgm"""

        return self.L1_AGBP_calc, self.L1_AET_calc

    @image_selection.setter
    def image_selection(self, date_p):

        """ Filter datasets selecting only images within the starting end ending date to be used for WPbm"""

        data_start = str(date_p[0])
        data_end = str(date_p[1])

        collection_agbp_filtered = self._L1_NPP_DEKADAL.filterDate(
            data_start,
            data_end)

        collection_aet_filtered = self._L1_AET_DEKADAL.filterDate(
            data_start,
            data_end)

        agbp_num = collection_agbp_filtered.size().getInfo()
        self.L1_logger.debug("AGBP selected %d" % agbp_num)

        aet_num = collection_aet_filtered.size().getInfo()
        self.L1_logger.debug("AET selected %d" % aet_num)

        self.L1_AGBP_calc = collection_agbp_filtered
        self.L1_AET_calc = collection_aet_filtered

    @property
    def multiply_npp(self, filtering_values):

        """ Sets the dataset to be used in conjunction with Actual Evapotranspiration for WPgb"""

        data_start = str(filtering_values[1])
        data_end = str(filtering_values[2])

        coll_npp_filtered = self._L1_NPP_DEKADAL.filterDate(
            data_start,
            data_end)
        coll_npp_multiplied = coll_npp_filtered.map(lambda npp_images: npp_images.multiply(filtering_values[0]))

        self.L1_AGBP_calc = coll_npp_multiplied

        return self.L1_AGBP_calc

    def image_processing(self, L1_AGBP_calc, L1_AET_calc):

        """wp_gross_biomass calculation returns all intermediate results besides the final wp_gross_biomass"""

        # Multiplied for generating AGBP from NPP using the costant 0.144
        l1_agbp_masked = L1_AGBP_calc.map(lambda lista: lista.multiply(0.144))

        # .multiply(10); the multiplier will need to be
        # applied on net FRAME delivery, not on sample dataset
        l1_agbp_summed = l1_agbp_masked.sum()

        # add image property (days in dekad) as band
        eta_dekad_added = L1_AET_calc.map(lambda imm_eta2: imm_eta2.addBands(
                                                 imm_eta2.metadata(
                                                 'days_in_dk')))        
        
        # get ET value, divide by 10 (as per FRAME spec) to get daily
        # value, and  multiply by number of days in dekad summed annuallyS
        ETa_dekad_multiplied = eta_dekad_added.map(lambda imm_eta3: imm_eta3.select('b1')
                                                   .divide(10)
                                                   .multiply(imm_eta3.select('days_in_dk'))).sum()

        # scale ETsum from mm/m² to m³/ha for WP calculation purposes
        ETaTotm3 = ETa_dekad_multiplied.multiply(10)

        # calculate biomass water productivity and add to map
        wp_gross_biomass = l1_agbp_summed.divide(ETaTotm3)

        return l1_agbp_summed, eta_dekad_added, ETa_dekad_multiplied, wp_gross_biomass

    def map_id_getter(self, wpbm_calc):

        """Generate a map id and a token for the calcualted WPbm raster file"""

        map_id = wpbm_calc.getMapId(self.VisPar_WPgb)
        return map_id

    def generate_ts(self, paese, data_start, data_end,dataset):

        """Generate a chart with the mean values calculated for a chosen country"""

        if dataset == 'agbp':
            collection = self._L1_AGBP_DEKADAL
        elif dataset == 'eta':
            collection = self._L1_AET_DEKADAL
        elif dataset == 'aet':
            collection = self._L1_AET250
        elif dataset == 'npp':
            collection = self._L1_NPP_DEKADAL

        just_country = self._COUNTRIES.filter(ee.Filter.eq('Country', paese))
        cut_poly = just_country.geometry()
        cut_bounding_box = cut_poly.bounds(1)

        collection_filtered = collection.filterDate(data_start, data_end).filterBounds(cut_bounding_box)

        def getMean(img):
            return img.reduceRegions(cut_bounding_box,
                                     ee.Reducer.mean(),
                                     200)

        ans = ee.FeatureCollection(collection_filtered.map(getMean)).flatten().aggregate_array('.all').getInfo()

        x_agbp = [x['properties']['mean'] for x in ans]
        labels_agbp = [x['id'][:8] for x in ans]
        lables_data = [datetime.datetime.strptime(label, "%Y%m%d").strftime('%Y-%m-%d') for label in labels_agbp]

        plt.plot(x_agbp)
        plt.title("timeserie")
        plt.xticks(range(len(labels_agbp)), lables_data, rotation=60)
        plt.show()


    def generate_areal_stats_annual_allcountries(self, year, ser='no output'):

        """Calculates several statistics for the Water Productivity pre-calculated 
            raster for all africa _COUNTRIES and the requested year"""

        self.L1_logger.debug("Statistics for year %s" % year)
        africa_bbox = ee.Geometry.Rectangle(-15.64, -33.58, 59.06, 25.96)
        filtered = self._COUNTRIES.filterBounds(africa_bbox)

        means = self._L1_WP_ANNUAL.reduceRegions(
            collection=filtered,
            reducer=ee.Reducer.mean(),
            scale=250,
        )

        reducers = ee.Reducer.minMax().combine(
            reducer2=ee.Reducer.stdDev(),
            sharedInputs=True
        )

        minMaxStds = self._L1_WP_ANNUAL.reduceRegions(
            collection=filtered,
            reducer=reducers,
            scale=250)

        features_m = means.getInfo()['features']
        features_mms = minMaxStds.getInfo()['features']

        df_m = pd.DataFrame(data=features_m[1:], columns=features_m[0])
        df_mms = pd.DataFrame(data=features_mms[1:], columns=features_mms[0])

        serie_m = df_m['properties'].apply(pd.Series)
        serie_mms = df_mms['properties'].apply(pd.Series)

        df_m = pd.DataFrame(serie_m[['fid', 'gaul_code', 'iso3', 'mean', 'name', 'region', 'subregion']])
        df_mms = pd.DataFrame(serie_mms[['iso3', 'min', 'max', 'stdDev']])

        df_stats = df_m.join(df_mms, lsuffix='_df_m', rsuffix='_df_mms')

        df_stats.to_csv(str(year) + '.csv')
        df_stats.to_json(str(year) + '.json')

        return df_stats


    def generate_areal_stats_dekad_country(self, chosen_country, wbpm_calc):

        """Calculates several statistics for the Water Productivity calculated raster for a chosen country"""
        just_country = self._COUNTRIES.filter(ee.Filter.eq('name', chosen_country))
        if just_country.size().getInfo() > 0:
            cut_poly = just_country.geometry()
            raster_nominal_scale = wbpm_calc.projection().nominalScale().getInfo()

            country_mean = wbpm_calc.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=cut_poly,
                scale=raster_nominal_scale,
                maxPixels=1e9
            )
            mean = country_mean.getInfo()
            mean['mean'] = mean.pop('b1')

            reducers = ee.Reducer.minMax().combine(
                reducer2=ee.Reducer.stdDev(),
                sharedInputs=True
            )

            # Use the combined reducer to get the min max and SD of the image.
            stats = wbpm_calc.reduceRegion(
                reducer=reducers,
                bestEffort=True,
                geometry=cut_poly,
                scale=raster_nominal_scale,
            )

            # Display the dictionary of band means and SDs.
            min_max_std = stats.getInfo()
            min_max_std['min'] = min_max_std.pop('b1_min')
            min_max_std['std'] = min_max_std.pop('b1_stdDev')
            min_max_std['max'] = min_max_std.pop('b1_max')
            min_max_std.update(mean)
            return min_max_std
        else:
            return 'no country'

    def image_visualization(self, viz_type, L1_AGBP, ETaColl3, WPbm):

        """Output the calculated WPbm using a map vizualizer or a chart (in this case
        every componeent of the calculation is plotted """

        if viz_type == 'm':

            ee.mapclient.addToMap(WPbm, self.VisPar_WPgb, 'Annual biomass water productivity')
            ee.mapclient.centerMap(17.75, 10.14, 4)

        elif viz_type == 'c':

            url_thumb_AGBP = L1_AGBP.getThumbUrl(self.VisPar_AGBPy)
            thumb_imag_AGBP = plt.imread(url_thumb_AGBP)

            url_thumb_ETaColl3 = ETaColl3.getThumbUrl(self.VisPar_ETay)
            thumb_imag_ETaColl3 = plt.imread(url_thumb_ETaColl3)

            url_thumb_WPbm = WPbm.getThumbUrl(self.VisPar_WPgb)
            thumb_imag_WPbm = plt.imread(url_thumb_WPbm)

            fig = plt.figure()
            ax1 = fig.add_subplot(2, 2, 1)
            ax1.imshow(thumb_imag_AGBP)
            ax1.set_title('AGBP')
            ax1.axis('off')

            ax2 = fig.add_subplot(2, 2, 2)
            ax2.imshow(thumb_imag_ETaColl3)
            ax2.set_title('ETaColl3')
            ax2.axis('off')

            ax3 = fig.add_subplot(2, 2, 3)
            ax3.imshow(thumb_imag_WPbm)
            ax3.set_title('WPbm')
            ax3.axis('off')

            plt.show()

    def generate_tiles(self):

        """INCOMPLETE  Split the calculated WPbm in 100 tiles facilitating the export"""

        driver = ogr.GetDriverByName('ESRI Shapefile')
        dir_shps = "tiles"
        os.chdir(dir_shps)
        file_shps = glob.glob("*.shp")

        allExportWPbm = []
        file_names = []

        for file_shp in file_shps:

            dataSource = driver.Open(file_shp, 0)

            if dataSource is None:
                sys.exit(('Could not open {0}.'.format(file_shp)))
            else:
                layer = dataSource.GetLayer(0)
                extent = layer.GetExtent()
                active_file = "tile_" + str(file_shp.split('.')[0]).split("_")[3]
                file_names.append(active_file)
                low_sx = extent[0], extent[3]
                up_sx = extent[0], extent[2]
                up_dx = extent[1], extent[2]
                low_dx = extent[1], extent[3]

                cut = [list(low_sx), list(up_sx), list(up_dx), list(low_dx)]

                Export_WPbm = {
                    "crs": "EPSG:4326",
                    "scale": 250,
                    'region': cut}
                allExportWPbm.append(Export_WPbm)

        return allExportWPbm, file_names

    def image_export(self, exp_type, wpgb):

        """ INCOMPLETE Export the 72 of the calculated wpgb to Google Drive,
        GEE Asset or generating a url for each tile"""

        driver = ogr.GetDriverByName('ESRI Shapefile')
        dir_shps = "tiles"
        os.chdir(dir_shps)
        list_shps = glob.glob("*.shp")

        for file_shp in list_shps:
            dataSource = driver.Open(file_shp, 0)
            if dataSource is None:
                sys.exit(('Could not open {0}.'.format(file_shp)))
            else:
                layer = dataSource.GetLayer(0)
                extent = layer.GetExtent()
                active_file = str(file_shp.split('.')[0])
                low_sx = extent[0], extent[3]
                up_sx = extent[0], extent[2]
                up_dx = extent[1], extent[2]
                low_dx = extent[1], extent[3]

                cut = []
                cut = [list(low_sx), list(up_sx), list(up_dx), list(low_dx)]

                Export_WPbm = {
                    "crs": "EPSG:4326",
                    "scale": 250,
                    'region': cut}

                if exp_type == 'u':
                    list_of_downloading_urls = []
                    try:
                        url_WPbm = wpgb.getDownloadUrl(Export_WPbm)
                        list_of_downloading_urls.append(url_WPbm)
                    except:
                        self.L1_logger.error("Unexpected error:", sys.exc_info()[0])
                        raise
                elif exp_type == 'd':
                    task = ee.batch.Export.image(wpgb,
                                                 active_file,
                                                 Export_WPbm)
                    task.start()
                    while task.status()['state'] == 'RUNNING':
                        # Perhaps task.cancel() at some point.
                        time.sleep(1)
                    self.L1_logger.info('Done.', task.status())

                elif exp_type == 'a':
                    active_file = "tile_" + str(file_shp.split('.')[0]).split("_")[3]
                    asset_temp = "projects/fao-wapor/testExpPython/JanMar2015/" + active_file
                    ee.batch.Export.image.toAsset(
                        image=wpgb,
                        description=active_file,
                        assetId=asset_temp,
                        crs="EPSG:4326",
                        scale= 250,
                        region=cut
                        ).start()

                elif exp_type == 'g':
                    active_file = "tile_" + str(file_shp.split('.')[0]).split("_")[3]
                    asset_temp = "projects/fao-wapor/testExpPython/" + active_file
                    self.L1_logger.info(active_file, asset_temp)
                elif exp_type == 'n':
                    print("Nothing yet")
                    pass
