from marmee.marmee import Marmee
from marmee.model import Input
from marmee.model.filter import Filter
from marmee.model.rule import Range, ExtentSchema, TemporalRule, Rule
from marmee.utils.parser import Stac
from ee import ImageCollection as EEImageCollection
from ee import Image as EEImage
from ee import Date as EEDate, EEException
import os
import ee
import dask
import json
import daiquiri
import datetime
import pendulum
from dask.delayed import delayed
from utils.helpers import ETI


class AETI(Marmee):

    def __init__(self, **kw):

        logger = daiquiri.getLogger(__name__, subsystem="algorithms")
        self.logger = logger

        # parallelize items computation for ETI inputs
        try:
            self.logger.info("==========INIT AETI Algorithm===========")
            self.logger.debug(
                "Named arguments kw =====> {0}".format(kw)
            )
            colls = [self._inputColl(
                self,
                coll_id
            ).compute() for coll_id in [kw["collE"], kw["collT"], kw["collI"]]]
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
        
        # temporal filter for AETI
        try:
            self.year = kw["year"]
            self.config = dict(
                export=kw["to_asset"],
                intermediate=kw["intermediate_outputs"],
                assetids=kw["dst_assets"]
            )
        except KeyError as exc:
            self.logger.error(
                "Fail to handle key from dictionary",
                exc_info=True
            )
            raise

        annualrule = self._inputAnnualTemporalRule(int(self.year))
        temporal_filter = self._inputTemporalFilter("temporal", annualrule)
        self._filters = temporal_filter

        # initialize outputs
        self._outputs = []
        self.errors = {}

        # Create a dict of EE ImageCollection for ETI
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
                "Item {0} is of type {1}".format(
                    inpt.stacobject.id, inpt.stacobject.type
                )
            )
            if inpt.stacobject.type == "FeatureCollection":
                try:
                    if "E_D" in inpt.stacobject.id:
                        inpt_dict["cE"] = EEImageCollection(
                            inpt.stacobject.id
                        )
                        self.logger.debug(
                            "ImageCollection info for {0} is =====>\n{1}".format(
                                inpt.stacobject.id,
                                inpt_dict["cE"].getInfo()
                            )
                        )
                    elif "T_D" in inpt.stacobject.id:
                        inpt_dict["cT"] = EEImageCollection(
                            inpt.stacobject.id
                        )
                        self.logger.debug(
                            "ImageCollection info for {0} is =====>\n{1}".format(
                                inpt.stacobject.id,
                                inpt_dict["cT"].getInfo()
                            )
                        )
                    elif "I_D" in inpt.stacobject.id:
                        inpt_dict["cI"] = EEImageCollection(
                            inpt.stacobject.id
                        )
                        self.logger.debug(
                            "ImageCollection info for {0} is =====>\n{1}".format(
                                inpt.stacobject.id,
                                inpt_dict["cI"].getInfo()
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

    def process_dekadal(self):
        """Calculate Dekadal AETI.
        """
        kwargs = self.coll
        kwargs.update(self.filter)
        collETI = ETI(**kwargs).getCollETI()

        if isinstance(collETI, dict):
            self.errors.update(collETI["errors"])
        else:
            self.logger.debug(
                "Config dictionary =====> {0}".format(
                    json.dumps(self.config)
                )
            )

            assetids = self.config["assetids"]
            self._tasks = {}

            for i in range(len(assetids)):
                # Case just one dekad has been asked from cli
                if len(assetids) == 1:
                    assetid = assetids[0]
                    export_img = EEImage(collETI.sort(
                        'system:index', True).toList(
                            1, int(os.path.basename(assetid)[-2:])
                        ).get(0)
                    )
                else:
                    export_img = EEImage(collETI.sort(
                        'system:index', True).toList(1, i).get(0)
                    )
                    assetid = export_img.getInfo()["id"]
                self.logger.debug(
                    "Information for exported image =====> {0}".format(
                        json.dumps(export_img.getInfo())
                    )
                )
                asset_name = os.path.basename(assetid)

                bands = export_img.getInfo()["bands"][0]
                dimensions = (bands["dimensions"][0], bands["dimensions"][1],)
                dekad_properties = export_img.getInfo()["properties"]
                
                # Set dekadal_props for export
                dekadal_props = self._setExportProperties(
                    asset_name, **dekad_properties
                )
                export_img_props = ee.Image.setMulti(
                    export_img, dekadal_props
                )
                properties = export_img_props.getInfo()
                self.logger.debug(
                    "New properties are =====>\n{0}".format(
                        json.dumps(properties)
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
                        image=export_img_props,
                        description=asset_name,
                        assetId=assetid,
                        crs=bands["crs"],
                        dimensions="{0}x{1}".format(
                            dimensions[0],
                            dimensions[1]
                        ),
                        maxPixels=dimensions[0] * dimensions[1],
                        crsTransform=str(bands["crs_transform"])
                    )
                    task.start()
                    self._tasks.update(dict(taskid=task.id))

                except (EEException, AttributeError) as e:
                    self.logger.error(
                        "Task export definition has failed!",
                        exc_info=True
                    )
                    raise

        return dict(
            tasks=self._tasks,
            outputs=self.outputs,
            errors=self.errors
        )

    @delayed
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

    def _setExportProperties(self, asset, **properties):
        try:
            res_props = {}
            self.logger.debug("Input properties are =====>\n{0}".format(
                json.dumps(properties)
            )
            )
            for key, value in properties.items():
                if key == "code":
                    res_props[key] = asset
                # elif key == "time_extent":
                #     res_props[key] = value
                # elif key == "time_resolution":
                #     res_props[key] = value
                # elif key == "n_days_extent":
                #     res_props[key] = value
                elif key == "no_data_value":
                    res_props[key] = "-9999"
                elif key == "system:asset_size":
                    pass
                # elif key == "system:time_start":
                #     res_props[key] = value
                # elif key == "system:time_end":
                #     res_props[key] = value
                else:
                    res_props[key] = value

            return res_props
        except KeyError as e:
            raise
