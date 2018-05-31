from marmee.marmee import Marmee
from marmee.model.input import Input
from marmee.model.filter import Filter
from marmee.model.rule import Range, ExtentSchema, TemporalRule, Rule
from marmee.utils.parser import Stac
from ee import ImageCollection as EEImageCollection
from ee import Image as EEImage
from ee import Date as EEDate, EEException
import ee
import dask
import json
import daiquiri
import datetime
from dask.delayed import delayed


class Common(Marmee):

    def __init__(self, **kw):

        logger = daiquiri.getLogger(__name__, subsystem="algorithms")
        self.logger = logger

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
                self.logger.error(eee.message)
            elif exc:
                self.logger.error(exc.message)
            raise
        
        # temporal filter for input component
        try:
            self.year = kw["year"]
            self.config = dict(
                export=kw["to_asset"],
                intermediate=kw["intermediate_outputs"],
                assetid=kw["dst_asset"]
            )
        except KeyError as exc:
            self.logger.error(exc.message)
            raise

        annualrule = self._inputAnnualTemporalRule(int(self.year))
        temporal_filter = self._inputTemporalFilter("temporal", annualrule)
        self._filters = temporal_filter

        # initialize outputs
        self._outputs = []

        # Create dicts of EE ImageCollection for input component
        flt_dict = {}
        inpt_dict = {}
        config_dict = {}
        # import ipdb ; ipdb.set_trace()
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
        """Calculate Annual image.
        """
        
        # Filtered collection
        self.logger.debug(
            "temporal_filter GEE object value is ======> {0}".format(
                self.filter["temporal_filter"]
            )
        )
        collFiltered = self.coll["collection"].filterDate(
            self.filter["temporal_filter"]['start'],
            self.filter["temporal_filter"]['end']
        ).sort('system:time_start', True)

        # properties
        size = collFiltered.size().getInfo()
            imagecrs = EEImage(collFiltered.first()).projection()
            scale = EEImage(
                collFiltered.first()
            ).projection().nominalScale().getInfo()
        if size = 36:
            

            componentColl = collFiltered.map(
                lambda img: img.addBands(img.metadata('n_days_extent'))
            )
            self.logger.debug(
                "componentColl info is =====> \n{0}".format(
                    componentColl.getInfo()
                )
            )
            component_annual = componentColl.map(
                lambda img_a: img_a.select('b1').divide(10).multiply(
                    img_a.select('n_days_extent')
                )
            )
            self.logger.debug(
                "component_annual info is =====> \n{0}".format(
                    component_annual.getInfo()
                )
            )

            sum_component_annual = component_annual.reduce(ee.Reducer.sum())
            self.logger.debug(
                "sum_component_annual info is =====> \n{0}".format(
                    sum_component_annual.getInfo()
                )
            )

            # set properties of image to be exported:
            # Int32, NoData = -9999, multiplier = 0.1
            sum_component_annual_int = sum_component_annual.multiply(
                10
            ).int32().unmask(
                -9999
            ).getInfo()
            
            bandNames = sum_component_annual.bandNames().getInfo()
            self.logger.debug(
                "bandNames info is =====> \n{0}".format(
                    bandNames
                )
            )
            
            b1proj = sum_component_annual.select(
                'b1_sum'
            ).projection().getInfo()
            self.logger.debug(
                "b1proj info is =====> \n{0}".format(
                    b1proj
                )
            )
            
            # get scale(in meters) information from band 1.
            b1sum_scale = sum_component_annual.select(
                'b1_sum'
            ).projection().nominalScale().getInfo()
            self.logger.debug(
                "b1sum_scale info is =====> \n{0}".format(
                    b1sum_scale
                )
            )

            properties = sum_component_annual.propertyNames().getInfo()
            self.logger.debug(
                "Properties are =====>\n{0}".format(
                    properties
                )
            )
            region = ee.Geometry.Polygon(
                [[[-30, -40],[65, -30],[65, 40],[-30, 40]]]
            ).getInfo()['coordinates']

            self.logger.debug(
                "Config dictionary =====> {0}".format(
                    json.dumps(self.config)
                )
            )
            assetid = self.config["assetid"]

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
                    image=sum_component_annual,
                    description='L1_E_16',
                    assetId=assetid,
                    crs='EPSG:4326',
                    scale=scale,
                    region=region,
                    maxPixels=9000000000
                )
                task.start()
                return dict(
                    tasks=dict(taskid=task.id),
                    outputs=self.outputs,
                    errors={}
                )
            except EEException as eee:
                self.logger.debug(
                    "Task export definition has failed."
                )
                raise
            

    
        else:
            self.logger.error("Filtered Collection has size {0}".format(size))
            raise click.Abort()

    # @delayed
    def _inputColl(self, collection_id):
        self.logger.debug("collection_id is =====> {0}".format(collection_id))
        try:
            gee_stac_obj = Stac(collection_id).parse()
            return Input(stacobject=Stac(collection_id).parse(), reducers=[])
        except EEException as eee:
            self.logger.debug(
                "Exception creating  Marmee object =====>\n{0}".format(eee.message)
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
