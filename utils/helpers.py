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
            self.ee_container,
            self.dst_collection()
        )

    def dst_image(self):
        return self.level + "_" + self.component + "_" + self.year[2:]

    def dst_asset_id(self):
        return os.path.join(
            self.ee_container,
            os.path.join(
                self.dst_collection(),
                self.dst_image()
            )
        )

    def _input_temporal_resolution(self):
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
            for dekad in range(1, 37):
                imgs.append(
                    self.dst_image() + str(dekad)
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
