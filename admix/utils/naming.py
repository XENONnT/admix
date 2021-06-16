


def make_did(run_number, dtype, hash):
    scope = 'xnt_%06d' % run_number
    dataset = "%s-%s" % (dtype, hash)
    return "%s:%s" % (scope, dataset)

def get_did(did):
    scope, dataset = did.split(':')
    number = int(scope.split('_')[-1])
    dtype = dataset.split('-')[0]
    hash = dataset.split('-')[-1]
    return number, dtype, hash

