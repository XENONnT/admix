#Here we have our decorators:

NameCollector = []
ClassCollector = {}

class Collector():
    def __init__(self, handover):
        if handover.__name__ not in NameCollector:
            NameCollector.append(handover.__name__)
            ClassCollector[str(handover.__name__)] = handover.__call__()

    def __call__(self, *args, **kwargs):
        pass
