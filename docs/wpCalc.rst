.. _wpCalc:

API Documentation
=================

.. module:: wapor_algorithms

For normal use, all you will have to do to get started is::

    from wpCalc import L1WaterProductivity

This will import the following:

* The class :L1WaterProductivity:`L1WaterProductivity` and its subclasses

* Several `constants`_ datasets uploaded or calculated in Google Earth Engine for WaPOR

The Water Productivity Calculation class
----------------------------------------

.. autoclass:: L1WaterProductivity
    :members:

Constants
---------

.. _constants:

    **Vector data to be used for exporting and statistics**
    ::
        _REGION = [[-25.0, -37.0], [60.0, -41.0], [58.0, 39.0], [-31.0, 38.0], [-25.0, -37.0]]
        _WSHEDS = ee.FeatureCollection('projects/fao-wapor/vectors/wapor_basins')
        _COUNTRIES = ee.FeatureCollection('projects/fao-wapor/vectors/wapor_countries')::

    **Level 1 Datasets**
    ::
        _L1_RET_DAILY = ee.ImageCollection("projects/fao-wapor/L1_RET")
        _L1_PCP_DAILY = ee.ImageCollection("projects/fao-wapor/L1_PCP")
        _L1_NPP_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_NPP")
        _L1_AET_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_AET")
        _L1_TFRAC_DEKADAL = ee.ImageCollection("projects/fao-wapor/L1_TFRAC")::

    *Land Cover for calculation to be changed in future*
    ::
        _L1_LCC = ee.ImageCollection("projects/fao-wapor/L1_LCC_preliminary")

    *PRE-Calculated annual data stored as collections - New Calculation allowed form 2017 onwards*
    ::
        _L1_AET_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_AET_Annual")
        _L1_AGBP_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_AGBP_Annual")
        _L1_T_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_T_Annual")
        _L1_WPgb_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_WPgb_Annual")
        _L1_WPnb_ANNUAL = ee.ImageCollection("projects/fao-wapor/L1_WPnb_Annual")

    **Level 2 Datasets**
    ::
        _L2_EANE_AET_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_EANE_AET")
        _L2_EANE_PHE_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_EANE_PHE")
        _L2_WANE_AET_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_WANE_AET")
        _L2_WANE_NPP_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_WANE_NPP")
        _L2_EANE_TFRAC_DEKADAL = ee.ImageCollection("projects/fao-wapor/L2_EANE_TFRAC")

