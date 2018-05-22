from marmee.marmee import Marmee
from marmee.model import Input
from marmee.utils.parser import Stac
from ee import ImageCollection as EEImageCollection
from ee import Date as EEDate
import ee
import dask
from dask.delayed import delayed


class AET(Marmee):

    def __init__(self, **kw):

        # parallelize item computation
        try:
            print kw
            colls = [self._inputColl(
                coll_id
            ) for coll_id in [kw["collI"], kw["collE"], kw["collT"]]]
            dask.compute(colls)
            self.set_inputs(colls)
        except ee.EEException as exc:
            raise
        
        self.year = kw['year']

    def process_annual(self):
        """Annual
        """
        eeItems = []
        for inpt in self.inputs:
            eeItems.append(
                EEImageCollection(inpt.item.id)
            )
        collAET = eeItems[0].map(eeItems[1]).map(eeItems[2])

        #TODO take temporal and spatial filter from self.filters

        start = EEDate(self.year + '-1-1')
        end = EEDate(self.year + '-12-31')
        
        collAETFiltered = collAET.filterDate(
            start,
            end
        ).sort('system:time_start', true)

        # properties
        imagecrs = EEImage(collAETFiltered.first()).projection()
        scale = ee.Image(
            collAETFiltered.first()
        ).projection().nominalScale().getInfo()
        size = collAETFiltered.size()

        ETaColl = collAETFiltered.map(
            self._n_days(collAETFiltered)
        )
        AET_annual = ETaColl.map(
            self._ETdk(ETaColl)
        )

        sum_AET_annual = AET_annual.reduce(ee.Reducer.sum())

        # set properties of image to be exported:
        # Int32, NoData = -9999, multiplier = 0.1
        sum_AET_annual_int = sum_AET_annual.multiply(
            10
        ).int32().unmask(
            -9999
        ).getInfo()
        bandNames = sum_AET_annual.bandNames().getInfo()
        b1proj = sum_AET_annual.select(
            'b1_sum'
        ).projection().getInfo()
        # get scale(in meters) information from band 1.
        b1sum_scale = sum_AET_annual.select(
            'b1_sum'
        ).projection().nominalScale().getInfo()

        properties = sum_AET_annual.propertyNames().getInfo()

    @delayed
    def _inputColl(collection_id):
        return Input(item=Stac(collection_id).parse(), reducers=[])

    def _inputAnnualTemporalRule(year):
        fromdate = datetime.date(year, 01, 01)
        todate = datetime.date(year, 12, 31)
        annual = Range(from_date=fromdate, to_date=todate)
        annualrule = TemporalRule(daterange=annual.dict)
        extentY = ExtentSchema().dump([annualrule], many=True)
        return Rule(identifier="annual", rule=extentY)

    def _inputTemporalFilter(filtername, trule):
        return Filter(name=filtername, rules=[trule])

    def _n_days(image):
        days = image.addBands(image.metadata('n_days_extent'))
        return days

    def _ETdk(image):
        mmdk = image.select('b1').divide(10).multiply(
            image.select('n_days_extent')
        )
        return mmdk
