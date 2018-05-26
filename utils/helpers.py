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

class TIME_RESOLUTION(Enum):
    dekadal = "DEKADAL"
    short_dekadal = "D"
    annual = "ANNUAL"
    short_annual = "A"
    everyday = "EVERYDAY"
    short_everyday = "E"
    seasonal = "SEASONAL"
    short_seasonal = "S"
