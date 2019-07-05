.. highlight:: shell

============
Installation
============

Required Pre-Installation:
--------------------------

Rucio - Scientific Data Management
++++++++++++++++++++++++++++++++++

The aDMIX tools uses activly the Rucio - Scientific Data Management tool to run uploads to grid locations according to your configuration. Therefore it is mandatory to install the Rucio client in the same Python environment such as aDMIX is installed. Further installations in different environments which requieres side loads (== sourcing from another Anaconda environment) is partially supported by the legacy module (Rucio CLI) but not recommended for further work since it is a) slow and the b) the legacy module may not fully developed.

The Rucio - Scientific Data Management tools is found on Github (https://github.com/rucio/rucio) and further information are given here https://rucio.cern.ch/

gfal
++++



Stable release
--------------

To install aDMIX, run this command in your terminal:

.. code-block:: console

    $ pip install admix

This is the preferred method to install aDMIX, as it will always install the most recent stable release. 

If you don't have `pip`_ installed, this `Python installation guide`_ can guide
you through the process.

.. _pip: https://pip.pypa.io
.. _Python installation guide: http://docs.python-guide.org/en/latest/starting/installation/


From sources
------------

The sources for aDMIX can be downloaded from the `Github repo`_.

You can either clone the public repository:

.. code-block:: console

    $ git clone git://github.com/XeBoris/admix

Or download the `tarball`_:

.. code-block:: console

    $ curl  -OL https://github.com/XeBoris/admix/tarball/master

Once you have a copy of the source, you can install it with:

.. code-block:: console

    $ python setup.py install


.. _Github repo: https://github.com/XeBoris/admix
.. _tarball: https://github.com/XeBoris/admix/tarball/master
