.. _api:

API Documentation
=================

.. module:: wapor_algorithms

For normal use, all you will have to do to get started is::

    from wpCalc import L1WaterProductivity

This will import the following:

* The class :L1WaterProductivity:`L1WaterProductivity` and its subclasses
* Several `constants`_ representing the datasets available in Google Earth Engine and the visualization parameters.

The Water Productivity class
-----------------------------

.. autoclass:: L1WaterProductivity
    :members:


Constants
---------

.. _constants:

- _L1_AGBP_SEASONAL is `projects/fao-wapor/L1_AGBP`
- _L1_AGBP_DEKADAL is `projects/fao-wapor/L1_AGBP250`
- _L1_ETa_DEKADAL is `projects/fao-wapor/L1_AET`
- _L1_AET250 is `users/lpeiserfao/AET250`
- _L1_NPP_DEKADAL is `projects/fao-wapor/L1_NPP250`

- VisPar_AGBPy
- VisPar_ETay
- VisPar_WPbm
