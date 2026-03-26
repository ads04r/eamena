from arches.app.models import models
from crossref.restful import Works

def resolve(doi):
    works = Works()
    try:
        return works(doi)
    except:
        return {}

def resourceinstance_from_doi(doi, graph_id=None):
    if graph_id is None:
        return models.ResourceInstance.objects.filter(resourceinstanceid__in=models.TileModel.objects.filter(data__icontains=doi).values('resourceinstance_id'))
    return models.ResourceInstance.objects.filter(graph_id=graph_id, resourceinstanceid__in=models.TileModel.objects.filter(data__icontains=doi).values('resourceinstance_id'))