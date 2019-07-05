=====
Usage
=====

advanced data management in XENON (aDMIX) allows you to run //stand alone// tasks which are decribed below. The purpose of the standalone tasks is to connect Rucio interactions (uploads, downloads and transfers) with the meta database (mongoDB interface). Furthermore, the aDMIX package offers a simple way interact with the Rucio catalogue or with the grid locations directly.

Standalone Tasks in aDMIX
+++++++++++++++++++++++++

Upload with MongoDB
-------------------

Run aDMIX to upload data sets (e.g. XENONnT plugins) into Rucio. 


Basic command outline:

.. code-block:: console
    
    admix UploadMongoDB --admix-config <CONFIG-FILE>




aDMIX as a Module
+++++++++++++++++



