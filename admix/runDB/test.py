# Just an example of how to use the different DB classes

from admix.runDB.xenon_runDB import *
import time

DBs = [XenonPymongoDB(), XenonRestDB()]

query = {'detector' : 'tpc',
         'source.type' : 'neutron_generator',
         'number' : {'$gt' : 17980 }}

for DB in DBs:
    t0 = time.clock()
    cursor = DB.query(query)

    for run in list(cursor):
        print(run['number'])

    tf = time.clock()

    print('%s took %0.3f seconds' % (DB.__class__.__name__, (tf - t0)))
