import os


class Name(object):
    
    def __init__(self, **kwargs):
        self.year = kwargs['year']
        self.component = kwargs['component']
        self.t_resolution = kwargs['temporal_resolution']
        self.level = kwargs['level']
        self.ee_container = kwargs['EE_WORKSPACE_WAPOR']

	def __repr__(self):
		return '<Name(={self.!r})>'.format(self=self)
    
    def src_collection(self):
        return self.level + "_" + self.component

    def dst_collection(self):
        return self.src_collection() + "_" + self.t_resolution

    def dst_assetcollection_id(self):
        return os.path.join(
            self.ee_container,
            self.dst_collection()
        )

    def dst_image(self):
        return self.component + "_" + self.year[2:]

    def dst_asset_id(self):
        return os.path.join(
            self.ee_container,
            os.path.join(
                self.dst_collection(),
                self.dst_image()
            )
        )