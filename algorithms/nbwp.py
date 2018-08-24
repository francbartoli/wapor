from marmee.marmee import Marmee
from marmee.model.input import Input
from marmee.model.filter import Filter
from marmee.model.rule import Range, ExtentSchema, TemporalRule, Rule
from marmee.utils.parser import Stac
from ee import ImageCollection as EEImageCollection
from ee import Image as EEImage
from ee import Filter as EEFilter
from ee import Date as EEDate, EEException
from gee_pheno.gee_pheno import Phenology
import ee
import os
import dask
import json
import daiquiri
import datetime
import pendulum
from dask.delayed import delayed


class NBWP(Marmee):

    def __init__(self, **kw):

        logger = daiquiri.getLogger(__name__, subsystem="algorithms")
        self.logger = logger
        self._name = "NBWP"

        try:
            if not kw.has_key("season"):
                self.coll_t_y = EEImageCollection(
                    kw["src_coll"].replace("AGBP", "T")
                )
            else:
                self.season = kw["season"]
                self.coll_t_y = EEImageCollection(
                    kw["src_coll"].replace("AGBP_S", "T_D")
                )
                self.coll_agbp_s = EEImageCollection(
                    kw["src_coll"]
                )
        except EEException as e:
            self.logger.error(
                "Failed to handle Google Earth Engine object",
                exc_info=True
            )
            raise

        # parallelize items computation for input component
        try:
            self.logger.debug(
                "Named arguments kw are =====> {0}".format(kw)
            )
            self.logger.info(
                "==========INIT Common Algorithm for {0}===========".format(
                    kw["src_coll"]
                )
            )
            # @TODO see why dask compute no longer works
            # colls = [self._inputColl(
            #     self,
            #     coll_id
            # ) for coll_id in [kw["src_coll"]]]
            # dask.compute(colls)
            # import ipdb; ipdb.set_trace()
            colls = [self._inputColl(kw["src_coll"])]
            self._inputs = colls
        except (EEException, KeyError) as (eee, exc):
            if eee:
                self.logger.error(
                    "Failed to handle Google Earth Engine object",
                    exc_info=True
                )
            elif exc:
                self.logger.error(
                    "Fail to handle key from dictionary",
                    exc_info=True
                )
            raise

        # temporal filter for input component
        try:
            self.year = kw["year"]
            self.config = dict(
                export=kw["to_asset"],
                intermediate=kw["intermediate_outputs"],
                assetid=kw["dst_asset"],
                ndvalue=kw["nodatavalue"]
            )
            if self.season:
                self.config.update(season=self.season)
        except KeyError as exc:
            self.logger.error("Error with dictionary key", exc_info=True)
            raise

        annualrule = self._inputAnnualTemporalRule(int(self.year))
        temporal_filter = self._inputTemporalFilter("temporal", annualrule)
        self._filters = temporal_filter

        # initialize outputs
        self._outputs = []
        self.errors = {}

        # Create dicts of EE ImageCollection for input component
        flt_dict = {}
        inpt_dict = {}
        config_dict = {}

        self.logger.debug(
            "Received inputs in STAC format are =====>\n{0}".format(
                self.inputs
            )
        )
        for inpt in self.inputs:
            self.logger.debug(
                "Stac object {0} is of type {1}".format(
                    inpt.stacobject.id, inpt.stacobject.type
                )
            )
            if inpt.stacobject.type == "FeatureCollection":
                try:
                    inpt_dict["collection"] = EEImageCollection(
                        inpt.stacobject.id
                    )
                    self.logger.debug(
                        "ImageCollection info for {0} is =====>\n{1}".format(
                            inpt.stacobject.id,
                            inpt_dict["collection"].getInfo()
                        )
                    )
                except EEException as eee:
                    self.logger.error(
                        "Failed to create ImageCollection {0}".format(
                            inpt.stacobject.id
                        ), exc_info=True
                    )
                    raise

        # it works for just one filter with only one temporal rule
        self.logger.debug(
            "Received filters are =====>\n{0}".format(
                self.filters.json
            )
        )
        for rul in self.filters.rules:
            for ext in rul.rule:
                if ext['type'] in 'temporal':
                    date_range = ext['daterange']
                    flt_dict['temporal_filter'] = self._eeDaterangeObj(
                        **date_range
                    )

        self.coll = inpt_dict
        self.filter = flt_dict
        self.config.update(config_dict)

    def process_annual(self):
        """Calculate Annual NBWP image.
        """

        self.logger.debug(
            "Config dictionary =====> {0}".format(
                json.dumps(self.config)
            )
        )

        self._tasks = {}

        # Filtered collection
        self.logger.debug(
            "temporal_filter GEE object value is ======> {0}".format(
                self.filter["temporal_filter"]
            )
        )
        collAGBPFiltered = self.coll["collection"].filterDate(
            self.filter["temporal_filter"]['start'],
            self.filter["temporal_filter"]['end']
        ).sort('system:time_start', True)
        collTFiltered = self.coll_t_y.filterDate(
            self.filter["temporal_filter"]['start'],
            self.filter["temporal_filter"]['end']
        ).sort('system:time_start', True)
        # nodatavalue -9999: consider only gte 0
        if self.config["ndvalue"] in "-9999":
            collAGBPFiltered = collAGBPFiltered.map(
                lambda image: image.mask(
                    image.select('b1_sum').gte(0)
                )
            )
        else:
            pass

        collTFiltered = collTFiltered.map(
            lambda image: image.mask(
                image.select('b1_sum').gte(100)
            )
        )

        # properties
        size_agbp = collAGBPFiltered.size().getInfo()
        size_t = collTFiltered.size().getInfo()
        if size_agbp and size_t is 1:
            first_t = EEImage(collTFiltered.first())
            first_t_info = first_t.getInfo()
            bands = first_t_info["bands"][0]
            dimensions = (bands["dimensions"][0], bands["dimensions"][1],)
            annual_properties = first_t_info["properties"]

            self.logger.debug(
                "First image T has following properties =====> \n{0}".format(
                    json.dumps(annual_properties)
                )
            )

            first_agbp = EEImage(collAGBPFiltered.first())
            self.logger.debug(
                "first_agbp info is =====> \n{0}".format(
                    first_agbp.getInfo()
                )
            )

            # no need to multiply T by 10 (to get metercubes)
            # because we should also divide by 10 to apply multiplier
            WPnb_annual = first_agbp.divide(first_t).multiply(1000)
            self.logger.debug(
                "WPnb_annual info is =====> \n{0}".format(
                    WPnb_annual.getInfo()
                )
            )

            WPnb_annual_int = WPnb_annual.unmask(
                -9999
            ).int32()

            bandNames = WPnb_annual_int.bandNames().getInfo()
            self.logger.debug(
                "bandNames info is =====> \n{0}".format(
                    bandNames
                )
            )

            self.logger.debug(
                "Config dictionary =====> {0}".format(
                    json.dumps(self.config)
                )
            )

            assetid = self.config["assetid"]
            asset_name = os.path.basename(assetid)

            # annual_props for export
            annual_props = self._setExportProperties(
                self.year, asset_name, **annual_properties
            )
            WPnb_annual_props = ee.Image.setMulti(
                WPnb_annual_int, annual_props
            )
            properties = WPnb_annual_props.getInfo()
            self.logger.debug(
                "New properties are =====>\n{0}".format(
                    json.dumps(properties)
                )
            )
            pyramid_policy = json.dumps({"{0}".format(
                properties["bands"][0]["id"]
            ): "mode"})
            self.logger.debug(
                "PyramidingPolicy is =====>\n{0}".format(
                    pyramid_policy
                )
            )
            # check if the asset already exists and eventually delete it
            if ee.data.getInfo(assetid):
                try:
                    ee.data.deleteAsset(assetid)
                except EEException as eee:
                    self.logger.debug(
                        "Trying to delete an assetId {0} \
which doesn't exist.".format(assetid)
                    )
                    raise
            # launch the task and return taskid
            try:
                task = ee.batch.Export.image.toAsset(
                    image=WPnb_annual_props,
                    description=asset_name,
                    assetId=assetid,
                    crs=bands["crs"],
                    dimensions="{0}x{1}".format(
                        dimensions[0],
                        dimensions[1]
                    ),
                    maxPixels=dimensions[0] * dimensions[1],
                    crsTransform=str(bands["crs_transform"]),
                    pyramidingPolicy=pyramid_policy
                )
                task.start()
                self._tasks.update(
                    {
                        "{0}".format(assetid): {
                            "taskid": task.id
                        }
                    }
                )
                return dict(
                    tasks=self._tasks,
                    outputs=self.outputs,
                    errors=self.errors
                )
            except (EEException, AttributeError) as e:
                self.logger.debug(
                    "Task export definition has failed with =====>\
\n{0}".format(e)
                )
                raise

        else:
            err_mesg = "Collection AGBP has size {0} while it should be 1".format(
                size_agbp
            )
            self.logger.error(err_mesg)
            n_errkey = str(len(self.errors) + 1)
            self.errors.update(
                {"{0}".format(n_errkey): "{0}".format(err_mesg)}
            )
            return dict(
                tasks={},
                outputs=self.outputs,
                errors=self.errors
            )

    def process_seasonal(self):
        """Calculate Seasonal NBWP image.
        """

        self.logger.debug(
            "Config dictionary =====> {0}".format(
                json.dumps(self.config)
            )
        )

        self._tasks = {}
        # Filtered collection
        self.logger.debug(
            "temporal_filter GEE object value is ======> {0}".format(
                self.filter["temporal_filter"]
            )
        )
        # AGBP collection
        collAGBP = self.coll["collection"].sort(
            'system:time_start', True
        )
        # nodatavalue -9999: consider only gte 0
        if self.config["ndvalue"] and "-9999" in self.config["ndvalue"]:
            collAGBP = collAGBP.map(
                lambda image: image.mask(
                    image.select('b1').gte(0)
                )
            )
        else:
            pass

        collTFiltered = self.coll_t_y.filterDate(
            self.filter["temporal_filter"]['start'],
            self.filter["temporal_filter"]['end']
        ).sort('system:time_start', True)

        # lambda to filter: pixel >=0 over the AGBP collection
        AGBPf = collAGBP.map(
            lambda image: image.updateMask(
                image.gte(0)
            )
        )
        # same annual plus season, be careful of gee number type
        AGBPs_Im = AGBPf.filter(
            EEFilter.eq('season', float(self.season))
        ).filterDate(
            self.filter["temporal_filter"]['start'],
            self.filter["temporal_filter"]['end']
        )
        # first element
        first_agbpsim = AGBPs_Im.first()

        # properties
        first_agbpsim_info = first_agbpsim.getInfo()
        bands = first_agbpsim_info["bands"][0]
        dimensions = (bands["dimensions"][0], bands["dimensions"][1],)
        seasonal_properties = first_agbpsim_info["properties"]

        # Set an instance of phenology collection
        phen = Phenology(
            # static configuration from file
            phe_coll="projects/fao-wapor/L2/L2_PHE_S",
            year=self.year,
            season=int(self.season)
        )
        # get phes_s image
        phes_s = phen.PHEsos_img
        # get phes_e image
        phes_e = phen.PHEeos_img
        # get phestart image
        phestart = phen.PHEstart
        # get pheend image
        pheend = phen.PHEend
        # get season_s image
        season_s = phen.season_s
        # get season_e image
        season_e = phen.season_e

        # calculate lenght of season in days
        season_days = season_e.subtract(season_s).divide(86400000)

        # adds band with own date to T Collection
        TColl_date = collTFiltered.map(
            lambda image: image.addBands(
                image.metadata('system:time_start')
            )
        )

        # filter T between PHEs and PHEe dates
        T_season = TColl_date.map(
            lambda image, seas=season_s, seae=season_e: image.updateMask(
                image.select(['system:time_start']).gte(seas).And(
                    image.select(['system:time_start']).lte(seae)
                )
            )
        )

        # get total ETa in m3/ha = (ETa_mm*10000)/1000)
        T_season_m3 = T_season.sum().multiply(10)
        # calculate NBWP
        NBWPSeason = first_agbpsim.divide(T_season_m3)

        # multiplier
        NBWP_seasonal = NBWPSeason.multiply(1000)
        self.logger.debug(
            "NBWP_seasonal info is =====> \n{0}".format(
                NBWP_seasonal.getInfo()
            )
        )

        NBWP_seasonal_int = NBWP_seasonal.select("b1").unmask(
            -9999
        ).int32()

        self.logger.debug(
            "Config dictionary =====> {0}".format(
                json.dumps(self.config)
            )
        )

        assetid = self.config["assetid"]
        asset_name = os.path.basename(assetid)

        # seasonal_props for export
        seasonal_props = self._setExportProperties(
            self.year, asset_name, **seasonal_properties
        )
        NBWP_seasonal_props = ee.Image.setMulti(
            NBWP_seasonal_int, seasonal_props
        )
        properties = NBWP_seasonal_props.getInfo()
        self.logger.debug(
            "New properties are =====>\n{0}".format(
                json.dumps(properties)
            )
        )
        pyramid_policy = json.dumps({"{0}".format(
            properties["bands"][0]["id"]
        ): "mode"})
        self.logger.debug(
            "PyramidingPolicy is =====>\n{0}".format(
                pyramid_policy
            )
        )
        # check if the asset already exists and eventually delete it
        if ee.data.getInfo(assetid):
            try:
                ee.data.deleteAsset(assetid)
            except EEException as eee:
                self.logger.debug(
                    "Trying to delete an assetId {0} \
which doesn't exist.".format(assetid)
                )
                raise
        # launch the task and return taskid
        try:
            task = ee.batch.Export.image.toAsset(
                image=NBWP_seasonal_props,
                description=asset_name,
                assetId=assetid,
                crs=bands["crs"],
                dimensions="{0}x{1}".format(
                    dimensions[0],
                    dimensions[1]
                ),
                maxPixels=dimensions[0] * dimensions[1],
                crsTransform=str(bands["crs_transform"]),
                pyramidingPolicy=pyramid_policy
            )
            task.start()
            self._tasks.update(
                {
                    "{0}".format(assetid): {
                        "taskid": task.id
                    }
                }
            )
            return dict(
                tasks=self._tasks,
                outputs=self.outputs,
                errors=self.errors
            )
        except (EEException, AttributeError) as e:
            self.logger.debug(
                "Task export definition has failed with =====>\
\n{0}".format(e)
            )
            raise

#         else:
#             err_mesg = "Collection AGBP has size {0} while it should be 1".format(
#                 size_agbp
#             )
#             self.logger.error(err_mesg)
#             n_errkey = str(len(self.errors) + 1)
#             self.errors.update(
#                 {"{0}".format(n_errkey): "{0}".format(err_mesg)}
#             )
#             return dict(
#                 tasks={},
#                 outputs=self.outputs,
#                 errors=self.errors
#             )

    # @delayed
    def _inputColl(self, collection_id):
        self.logger.debug("collection_id is =====> {0}".format(collection_id))
        try:
            gee_stac_obj = Stac(collection_id).parse()
            return Input(stacobject=gee_stac_obj, reducers=[])
        except EEException as eee:
            self.logger.error(
                "Exception creating Marmee object {0}".format(
                    gee_stac_obj.id
                ),
                exc_info=True
            )
            raise

    def _inputAnnualTemporalRule(self, year):
        fromdate = datetime.date(year, 01, 01)
        todate = datetime.date(year, 12, 31)
        annual = Range(from_date=fromdate, to_date=todate)
        annualrule = TemporalRule(daterange=annual.dict)
        extentY = ExtentSchema().dump([annualrule], many=True)
        return Rule(identifier="annual", rule=extentY)

    def _inputTemporalFilter(self, filtername, trule):
        return Filter(name=filtername, rules=[trule])

    def _eeDaterangeObj(self, **kwargs):
        try:
            start = kwargs["from_date"]
            self.logger.debug(
                "from_date value is =====> {0}".format(start)
            )
            end = kwargs["to_date"]
            self.logger.debug(
                "to_date value is =====> {0}".format(end)
            )
            return {
                'start': EEDate(start),
                'end': EEDate(end)
            }
        except (KeyError, EEException) as (err, exc):
            raise

    def _setExportProperties(self, year, asset, **properties):
        try:
            res_props = {}
            self.logger.debug("Input properties are =====>\n{0}".format(
                json.dumps(properties)
            )
            )
            for key, value in properties.items():
                if key == "code":
                    res_props[key] = asset
                elif key == "time_extent":
                    res_props[key] = "from {0}-01-01 to {0}-12-31".format(
                        year
                    )
                elif key == "time_resolution":
                    res_props[key] = "YEAR".format(
                        str(self._days_in_year(year))
                    )
                elif key == "n_days_extent":
                    res_props[key] = "{0}.0".format(
                        str(self._days_in_year(year))
                    )
                elif key == "multiplier":
                    res_props[key] = 0.001
                elif key == "no_data_value":
                    res_props[key] = "-9999"
                elif key == "data_type":
                    res_props[key] = "{0}bit Unsigned Integer".format(
                        "32"
                    )
                elif key == "unit":
                    res_props[key] = "kgDM/" + u"m\u00b3"
                elif key == "system:asset_size":
                    pass
                elif key == "system:time_start":
                    res_props[key] = ee.Date.fromYMD(
                        int(year), 1, 1
                    ).millis().getInfo()
                elif key == "system:time_end":
                    res_props[key] = ee.Date.fromYMD(
                        int(year), 12, 31
                    ).millis().getInfo()
                else:
                    res_props[key] = properties[key]

            return res_props
        except KeyError as e:
            raise

    def _days_in_year(self, y):
        if pendulum.datetime(int(y), 1, 1).is_leap_year():
            return 366
        else:
            return 365
