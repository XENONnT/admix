
from ..utils import RAW_DTYPES
from ..uploader import upload
from .admix_daemon import AdmixDaemon


class UploadDaemon(AdmixDaemon):
    # TODO fix this, just a guess
    query = {'$and': [{'data': {'$elemMatch': {'status': 'transferred',
                                               'host': {'$regex': 'eb'}
                                               }
                                }
                       },
                      {'data': {'$not': {'$elemMatch': {'host': 'rucio-catalogue',
                                                        'type': {'$in': RAW_DTYPES}
                                                       }
                                          }
                               }
                      }
                    ]
             }

    projection = {'number': 1, 'data': 1}



    def do_task(self, rundoc):
        # TODO
        # get path of data to upload

        raise NotImplementedError
