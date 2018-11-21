import os
from enum import Enum
import ee
import click
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


class CommonName(Name):
    """ Manage Common name convention on GEE.

        Examples
        --------
        dataset: E (Evaporation)

        input:
            {L1_E_D,}
            year: 2017
            level: L1,L2,L3
            component: E,T,I
            temporal_resolution: D

        output:
            {L1_E_A/L1_E_17}
            level: L1
            component: E
            temporal_resolution: A
    """

    def __init__(self, **kwargs):
        if "area_code" in kwargs and not (
            kwargs["area_code"] == "NA"
        ):
            self.area = kwargs["area_code"]
        else:
            self.area = None
        self.year = kwargs['year']
        self.component = kwargs['component']
        self.t_resolution = kwargs['temporal_resolution']
        self.level = kwargs['level']
        self.ee_container = kwargs['EE_WORKSPACE_WAPOR']

    def dst_image(self):
        img = "{0}_{1}_{2}".format(
            self.level,
            self.component,
            self.year[2:]
        )
        if self.area:
            return "{0}_{1}".format(img, self.area)
        else:
            return img


class AGBPName(Name):
    """ Manage AGBP name convention on GEE.

        Example dataset: NPP

        input:
            {L1_NPP_D,}
            year: 2017
            level: L1
            component: NPP
            temporal_resolution: D

        output:
            {L1_AGBP_A/L1_AGBP_17}
            level: L1
            component: AGBP
            temporal_resolution: A
    """

    def __init__(self, **kwargs):
        self.year = kwargs['year']
        self.component = kwargs['component']
        self.t_resolution = kwargs['temporal_resolution']
        self.level = kwargs['level']
        self.ee_container = kwargs['EE_WORKSPACE_WAPOR']

    def __repr__(self):
        return '<AGBPName(={self.!r})>'.format(self=self)

    def dst_collection(self):
        return self.level + "_" + "AGBP" + "_" + self.t_resolution

    def dst_image(self):
        return self.level + "_" + "AGBP" + "_" + self.year[2:]


class NBWPName(Name):
    """ Manage NBWP name convention on GEE.

        Examples
        --------
        dataset: AGBP A

        input:
            {L1_AGBP_A,}
            year: 2017
            level: L1
            component: AGBP
            temporal_resolution: A

        output:
            {L1_NBWP_A/L1_NBWP_17}
            level: L1
            component: NBWP
            temporal_resolution: A

        dataset: AGBP S

        input:
            {L2_AGBP_S, L2_AETI_A}
            year: 2017
            level: L2
            component: AGBP
            temporal_resolution: S
            season: 1

        output:
            {L2_NBWP_S/L2_NBWP_17s1}
            year: 2017
            level: L2
            component: NBWP
            temporal_resolution: S
            season: 1
    """

    def __init__(self, **kwargs):
        self.year = kwargs['year']
        if "season" in kwargs:
            self.season = kwargs["season"]
        self.component = kwargs['component']
        self.t_resolution = kwargs['temporal_resolution']
        self.level = kwargs['level']
        self.ee_container = kwargs['EE_WORKSPACE_WAPOR']

    def __repr__(self):
        return '<NBWPName(={self.!r})>'.format(self=self)

    def src_collection(self):
        return "{0}_{1}_{2}".format(
            self.level, self.component, self.t_resolution
        )

    def dst_collection(self):
        return "{0}_NBWP_{1}".format(
            self.level, self.t_resolution
        )

    def dst_image(self):
        dstimg = "{0}_NBWP_{1}".format(
            self.level, self.year[2:]
        )
        try:
            if self.season:
                return "{0}s{1}".format(
                    dstimg, self.season
                )
            else:
                return dstimg
        except AttributeError as e:
            raise


class GBWPName(Name):
    """ Manage GBWP name convention on GEE.

        Examples
        --------
        dataset: AGBP A

        input:
            {L1_AGBP_A,}
            year: 2017
            level: L1
            component: AGBP
            temporal_resolution: A
            season: -1

        output:
            {L1_GBWP_A/L1_GBWP_17}
            level: L1
            component: GBWP
            temporal_resolution: A

        dataset: AGBP S

        L2:
            input:
                {L2_AGBP_S, L2_AETI_A}
                year: 2017
                level: L2
                component: AGBP
                temporal_resolution: S
                season: 1

            output:
                {L2_GBWP_A/L2_GBWP_17s1}
                year: 2017
                level: L2
                component: GBWP
                temporal_resolution: S
                season: 1

        L3:
            input:
                {L3_AGBP_S, L3_AETI_A}
                year: 2017
                level: L3
                component: AGBP
                temporal_resolution: S
                season: 1
                area: AWA

            output:
                {L3_GBWP_S/L3_GBWP_17s1_AWA}
                year: 2017
                level: L3
                component: GBWP
                temporal_resolution: S
                season: 1
                area: AWA
    """

    def __init__(self, **kwargs):
        self.year = kwargs['year']
        if "season" in kwargs:
            self.season = kwargs["season"]
        self.component = kwargs['component']
        self.t_resolution = kwargs['temporal_resolution']
        self.level = kwargs['level']
        self.area = kwargs['area_code']
        self.ee_container = kwargs['EE_WORKSPACE_WAPOR']

    def __repr__(self):
        return '<GBWPName(={self.!r})>'.format(self=self)

    def src_collection(self):
        return "{0}_{1}_{2}".format(
            self.level, self.component, self.t_resolution
        )

    def dst_collection(self):
        return "{0}_GBWP_{1}".format(
            self.level, self.t_resolution
        )

    def dst_image(self):
        dstimg = "{0}_GBWP_{1}".format(
            self.level, self.year[2:]
        )
        try:
            if self.season and not(self.season == "-1"):
                dstimg = "{0}s{1}".format(
                    dstimg, self.season
                )
            if self.level == "L3":
                if not self.area == "NA":
                    dstimg = "{0}_{1}".format(
                        dstimg, self.area
                    )
                else:
                    raise click.Abort(
                        "An area code different from NA is required for L3 GBWP"
                    )
            return dstimg
        except AttributeError as e:
            raise


class AETIName(Name):
    """ Manage AETI name convention on GEE.

        Example dataset: ETI

        input:
            {L1_E_D,L1_T_D,L1_I_D}
            year: 2017
            level: L1,L2,L3
            component: E,T,I
            temporal_resolution: D

        output:
            {L1_AETI_D/L1_AETI_1706}
            level: L1
            component: ETI
            temporal_resolution: D
    """

    def __init__(self, **kwargs):
        if "dekad" in kwargs:
            self.single_dekad = kwargs["dekad"]
        if "area_code" in kwargs and not (
            kwargs["area_code"] == "NA"
        ):
            self.area = kwargs["area_code"]
        else:
            self.area = None
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

        if self.area:
            imgs = ["{0}_{1}".format(img, self.area) for img in imgs]

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
            if kwargs.has_key("area"):
                self.filter_area = kwargs["area"]
            else:
                self.filter_area = None
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
        # Additional filter area for L3
        farea = ee.Filter.eq('area_code', self.filter_area)
        if self.filter_area:
            collEFiltered = collEFiltered.filter(farea)
            collTFiltered = collTFiltered.filter(farea)
            collIFiltered = collIFiltered.filter(farea)

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
            for k, v in size.items():
                if v > 0:
                    pass
                else:
                    err_mesg = "Collection {0} has size {1} while\
it should be greater than 0".format(
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
