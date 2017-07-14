#! /usr/bin/env python

import ee
import ee.mapclient

ee.Initialize()

# Print the information for an image asset.
image = ee.Image('srtm90_v4')
print(image.getInfo())

# ee.mapclient.addToMap(image,{}, str(image))

