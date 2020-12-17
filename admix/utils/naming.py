

def make_did(run_number, dtype, hash):
    # batch scopes into bunches of 10k
    scope = "xnt_%02d" % (run_number / 10000)
    dataset = "%06d-%s-%s" % (run_number, dtype, hash)
    return "%s:%s" % (scope, dataset)


def make_did_old(run_number, dtype, hash):
    scope = 'xnt_%06d' % run_number
    dataset = "%s-%s" % (dtype, hash)
    return "%s:%s" % (scope, dataset)

