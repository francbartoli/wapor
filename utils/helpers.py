import os
from enum import Enum
import ee
from ee import Filter as EEFilter
from ee import ImageCollection as EEImageCollection
from ee import Image as EEImage
from ee import EEException


class Name(object):
    """ Manage name convention on GEE.

        Example dataset: E (Evaporation)
        
        input:
            year: 2017
            level: L1
            component: E
            temporal_resolution: D
        
        output:
            level: L1
            component: E
            temporal_resolution: A
    """
    
    def __init__(self, **kwargs):
        self.year = kwargs['year']
        self.component = kwargs['component']
        self.t_resolution = kwargs['temporal_resolution']
        self.level = kwargs['level']
        self.ee_container = kwargs['EE_WORKSPACE_WAPOR']

	def __repr__(self):
		return '<Name(={self.!r})>'.format(self=self)
    
    def src_collection(self):
        return self.level + "_" + self.component + "\
_" + self._input_temporal_resolution() 

    def dst_collection(self):
        return self.level + "_" + self.component + "_" + self.t_resolution

    def dst_assetcollection_id(self):
        return os.path.join(
            os.path.join(
                self.ee_container,
                self.level
            ),
            self.dst_collection()
        )

    def dst_image(self):
        return self.level + "_" + self.component + "_" + self.year[2:]

    def dst_asset_id(self):
        return os.path.join(
            os.path.join(
                self.ee_container,
                self.level
            ),
            os.path.join(
                self.dst_collection(),
                self.dst_image()
            )
        )

    def _input_temporal_resolution(self):
        # Any Dekadal to Annual
        if self.t_resolution == TIME_RESOLUTION.short_annual.value:
            return TIME_RESOLUTION.short_dekadal.value


class AETIName(Name):
    """ Manage AETI name convention on GEE.

        Example dataset: ETI 
        
        input:
            {L1_E_D,L1_T_D,L1_I_D}
            year: 2017
            level: L1
            component: E,T,I
            temporal_resolution: D
        
        output:
            {L1_AETI_D/L1_AETI_1706}
            level: L1
            component: ETI
            temporal_resolution: D
    """

    def __init__(self, **kwargs):
        if kwargs.has_key("dekad"):
            self.single_dekad = kwargs["dekad"]
        self.year = kwargs['year']
        self.component = kwargs['component']
        self.t_resolution = kwargs['temporal_resolution']
        self.level = kwargs['level']
        self.ee_container = kwargs['EE_WORKSPACE_WAPOR']

    def src_collection(self):
        res = []
        eti = self.component.strip("A")
        for scomp in tuple(eti):
            res.append(self.level + "_" + scomp + "\
_" + self._input_temporal_resolution())
        return res

    def _input_temporal_resolution(self):
        # E,T,I, Dekadal to AETI Dekadal
        if self.t_resolution == TIME_RESOLUTION.short_dekadal.value:
            return TIME_RESOLUTION.short_dekadal.value

    def dst_images(self):
        imgs = []
        if self._input_temporal_resolution(
    ) == TIME_RESOLUTION.short_dekadal.value:
            if self.single_dekad:
                imgs.append(
                    self.dst_image() + "%.2d" % int(self.single_dekad)
                )
            else:
                for dekad in range(1, 37):
                    imgs.append(
                        self.dst_image() + "%.2d" % dekad
                    )
            return imgs

    def dst_asset_ids(self):
        asset_ids = []
        if self._input_temporal_resolution(
    ) == TIME_RESOLUTION.short_dekadal.value:
            for dst_image in self.dst_images():
                asset_ids.append(
                        os.path.join(
                        os.path.join(
                            self.ee_container,
                            self.level
                        ),
                        os.path.join(
                            self.dst_collection(),
                            dst_image
                        )
                    )
                )
            return asset_ids


class TIME_RESOLUTION(Enum):
    dekadal = "DEKADAL"
    short_dekadal = "D"
    annual = "ANNUAL"
    short_annual = "A"
    everyday = "EVERYDAY"
    short_everyday = "E"
    seasonal = "SEASONAL"
    short_seasonal = "S"

class ETI(object):
    def __init__(self, **kwargs):
        try:
            if isinstance(kwargs["cE"], EEImageCollection):
                self.ce = kwargs["cE"]
            if isinstance(kwargs["cT"], EEImageCollection):
                self.ct = kwargs["cT"]
            if isinstance(kwargs["cI"], EEImageCollection):
                self.ci = kwargs["cI"]
            self.tfilter = kwargs["temporal_filter"]
        except KeyError as exc:
            raise KeyError("A key element {0} for ETI is missing".format(
                exc.args[0]
            ))
        except EEException as eee:
            raise

    def getCollETI(self):
        """Generate ETI collection.
        """
        start = self.tfilter['start']
        end = self.tfilter['end']
        # Additional mask for pixel value > 250
        collEFiltered = self.ce.filterDate(
            start,
            end
        ).sort('system:time_start', True).map(
            lambda image: image.mask(
                image.select('b1').lte(250)
            )
        )
        # Additional mask for pixel value > 250
        collTFiltered = self.ct.filterDate(
            start,
            end
        ).sort('system:time_start', True).map(
            lambda image: image.mask(
                image.select('b1').lte(250)
            )
        )
        # Additional mask for pixel value > 250
        collIFiltered = self.ci.filterDate(
            start,
            end
        ).sort('system:time_start', True).map(
            lambda image: image.mask(
                image.select('b1').lte(250)
            )
        )

        # sizes
        size_err_dict = {}
        sizeE = {
            os.path.basename(
                collEFiltered.getInfo()["id"]
            ): collEFiltered.size().getInfo()
        }
        sizeT = {
            os.path.basename(
                collTFiltered.getInfo()["id"]
            ): collTFiltered.size().getInfo()
        }
        sizeI = {
            os.path.basename(
                collIFiltered.getInfo()["id"]
            ): collIFiltered.size().getInfo()
        }

        for size in (sizeE, sizeI, sizeT):
            for k,v in size.items():
                if v is 36:
                    pass
                else:
                    err_mesg = "Collection {0} has size {1} while it should be 36".format(
                        k,
                        v
                    )
                    n_errkey = str(len(size_err_dict) + 1)
                    size_err_dict.update(
                        {"{0}".format(n_errkey): "{0}".format(err_mesg)}
                    )
        if not size_err_dict.keys():
            # Join E and T Collections
            _joinFilteredET = self._joinFilteredET(
                collEFiltered, collTFiltered
            )
            joinCollET = _joinFilteredET.map(
                lambda image: image.rename('Eband', 'Tband')
            )
            # calculate ET and add it
            collET = joinCollET.map(
                lambda image: EEImage.cat(
                    image.select("Eband"),
                    image.select("Tband"),
                    image.select("Eband").add(
                        image.select("Tband")
                    ).rename("ETband")
                )
            )

            # Join ET and I Collections
            _joinFilteredETI = self._joinFilteredETI(
                collET, collIFiltered
            )
            joinCollETI = _joinFilteredETI.map(
                lambda image: image.rename(
                    'Eband', 'Tband', 'ETband', 'Iband'
                )
            )
            # calculate ETI and add it
            collETI = joinCollETI.map(
                lambda image: EEImage.cat(
                    image.select("Eband"),
                    image.select("ETband").add(
                        image.select("Iband")
                    ).rename("b1"),
                    image.select("Tband"),
                    image.select("ETband"),
                    image.select("Iband")
                ).select("b1") # it only returns b1 band in result
            )

            return collETI

        else:
            return dict(errors=size_err_dict)

    def _joinFilteredET(self, e, t):
        time_filter = EEFilter.equals(
            leftField="system:time_start",
            rightField="system:time_start"
        )
        join = ee.Join.inner()
        joinCollET = EEImageCollection(
            join.apply(
                e, t, time_filter
            )
        )
        return joinCollET.map(
            lambda element: EEImage.cat(
                element.get('primary'),
                element.get('secondary')
            )
        ).sort('system:time_start')

    def _joinFilteredETI(self, et, i):
        time_filter = EEFilter.equals(
            leftField="system:time_start",
            rightField="system:time_start"
        )
        join = ee.Join.inner()
        joinCollETI = EEImageCollection(
            join.apply(
                et, i, time_filter
            )
        )
        return joinCollETI.map(
            lambda element: EEImage.cat(
                element.get('primary'),
                element.get('secondary')
            )
        ).sort('system:time_start')
