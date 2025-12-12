from django.contrib.auth.models import User
from django.contrib.gis.geos import GEOSGeometry
from django.http import HttpRequest
from django.core.exceptions import ValidationError, ObjectDoesNotExist
from django.conf import settings
from arches.app.models.models import GraphModel, Node, ResourceInstance, TileModel, Language
from arches.app.models.concept import Concept, get_preflabel_from_valueid, get_valueids_from_concept_label
from arches.app.views import search
from django.core.management.base import BaseCommand
from django.contrib.gis.geos.error import GEOSException
from eamena.bulk_uploader import HeritagePlaceBulkUploadSheet, GridSquareBulkUploadSheet
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError, NotFoundError
from geomet import wkt
import json, os, sys, logging, re, uuid, hashlib, datetime, warnings

from eamena.bulk_uploader.util import list_nodes, convert, translate, validate, unflatten, prerequisites, annotate, summary, undo

logger = logging.getLogger(__name__)

class Command(BaseCommand):
	"""
	Commands for managing the import of EAMENA BUS files

	"""
	def add_arguments(self, parser):

		languages = []
		for l in settings.LANGUAGES:
			languages.append(l[0])
		if len(languages) == 0:
			languages = ['en'] # Make sure there's at least one language.

		parser.add_argument(
			"-o",
			"--operation",
			action="store",
			dest="operation",
			default="",
			choices=[
				"list_nodes",
				"convert",
				"translate",
				"validate",
				"unflatten",
				"prerequisites",
				"annotate",
				"summary",
				"undo"
			],
			help="Operation Type; 'list_nodes'=Lists all the valid nodes in a graph.'convert'=Converts an XLSX bulk upload sheet into Arches JSON. 'validate'=Inspects an XLSX bulk upload sheet and lists errors. 'unflatten'=Dumps the intermediate data format, in the correct structure but without validating concepts.'prerequisites'=Returns an Arches JSON file containing all the prerequisite objects (grid ids, etc) that do not already exist in the database.'annotate'=Takes an Arches import file and outputs the same file but with extra properties (which are ignored by Arches) describing the field names and concepts, making the file much easier for a human to read.'summary'=Returns a list of UUIDs of imported items, and their EAMENA IDs.'undo'=Takes a generated Arches JSON business data file as an input, and deletes all UUIDs referenced within, effectively undoing a bulk upload."
		)

		parser.add_argument(
			"-w",
			"--warnings",
			action="store",
			dest="warn_mode",
			default="warn",
			choices=["warn", "ignore", "strict"],
			help="Warn mode; 'warn'=Write warnings to STDERR, but ultimately ignore them. 'ignore'=Silently ignore warnings altogether. 'strict'=Treat warnings as errors, and stop if any are encountered."
		)

		parser.add_argument(
			"-l",
			"--language",
			action="store",
			dest="bus_language",
			default=languages[0],
			choices=languages,
			help="Language of BUS file."
		)

		parser.add_argument(
			"-a",
			"--append",
			action="store",
			dest="append_mode",
			default="new",
			choices=["new", "append"],
			help="Append mode; 'new'=Don't append, generate new UUIDs for items 'append'=Append data to existing records, using UNIQUEID as an identifier."
		)

		parser.add_argument(
			"-s", "--source", action="store", dest="source", default="", help="BUS file for processing.",
		)

		parser.add_argument(
			"-d", "--dest_dir", action="store", dest="dest_dir", default="", help="Directory where you want to save exported files. Omitting this argument dumps to STDOUT."
		)

		parser.add_argument(
			"-g",
			"--graph",
			action="store",
			dest="graph",
			default=False,
			help="The graphid of the resources you would like to import/convert.",
		)

	def handle(self, *args, **options):

		data = []
		self.errors = []
		self.warnings = []

		warn_mode = 'warn'

		if options['dest_dir']:
			if not(os.path.exists(options['dest_dir'])):
				self.__error("", "Output path not found: " + options['dest_dir'])
			elif not(os.path.isdir(options['dest_dir'])):
				self.__error("", "Output path is not a directory: " + options['dest_dir'])

		if options['warn_mode'] != '':

			warn_mode = options['warn_mode']

		if options['operation'] == '':

			self.__error("", "No operation selected. Use --operation")

		if options['operation'] == 'convert':

			try:
				rm = GraphModel.objects.get(graphid=options['graph'])
			except GraphModel.DoesNotExist:
				rm = None

			if rm is None:
				self.__error("", "Invalid or missing graph UUID. Use --graph")
				model_name = ''
			else:
				model_name = str(rm.name)

			if model_name == 'Heritage Place':

				translated_data = self.__translate_heritage_place(options)
				if self.__check_translated_data(translated_data):
					resources = self.__convert_translated_data(translated_data, options)
					mapped_resources = self.__map_resources(resources, options)
					business_data = {"resources": mapped_resources}
					data = {"business_data": business_data}

			if model_name == 'Grid Square':

				translated_data = self.__translate_grid_square(options)
				resources = self.__convert_translated_grid_square(translated_data, options)
				business_data = {"resources": resources}
				data = {"business_data": business_data}

		if options['operation'] == 'validate':

			translated_data = self.__translate_heritage_place(options)
			if self.__check_translated_data(translated_data):
				resources = self.__convert_translated_data(translated_data, options)
				mapped_resources = self.__map_resources(resources, options)
				business_data = {"resources": mapped_resources}

				if (len(self.warnings) + len(self.errors)) == 0:
					if len(business_data['resources']) == 0:
						self.__warn('', 'No valid data found', 'The validator has been through the file provided and cannot find any valid data.')

			if warn_mode != 'ignore':

				self.errors = self.errors + self.warnings
				self.warnings = []

			convert = lambda text: int(text) if text.isdigit() else text.lower()
			natsort_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key[0]) ]
			data = self.errors
			data.sort(key=natsort_key)
			self.errors = []

		if options['operation'] == 'prerequisites':

			translated_data = self.__translate_heritage_place(options)
			resources = self.__convert_translated_data(translated_data, options)
			prerequisites = self.__get_prerequisites(resources, options)
			business_data = {"resources": prerequisites}
			data = {"business_data": business_data}

		if options['operation'] == 'unflatten':

			data = unflatten(options['graph'], options['source'], options['bu_language'], options['warn_mode'], (options['append_mode'] == 'append'))

		if options['operation'] == 'translate':

			try:
				rm = GraphModel.objects.get(graphid=options['graph'])
			except GraphModel.DoesNotExist:
				rm = None

			if rm is None:
				self.__error("", "Invalid or missing graph UUID. Use --graph")
				model_name = ''
			else:
				model_name = str(rm.name)

			if model_name == 'Heritage Place':
				data = self.__translate_heritage_place(options)

			if model_name == 'Grid Square':
				data = self.__translate_grid_square(options)

			for i in range(0, len(data)):
				if '_' in (data[i]):
					del(data[i]['_'])

		if options['operation'] == 'list_nodes':

			data = list_nodes(options['graph'], options['bu_language'], options['warn_mode'])

		if options['operation'] == 'annotate':

			fn = options['source']
			fp = open(fn, 'r')
			data = json.loads('\n'.join(fp.readlines()))
			fp.close()
			nodes = self.__list_nodes(options)

			for r in range(0, len(data['business_data']['resources'])):
				for t in range(0, len(data['business_data']['resources'][r]['tiles'])):

					tileid = data['business_data']['resources'][r]['tiles'][t]['tileid']
					nodegroup_id = data['business_data']['resources'][r]['tiles'][t]['nodegroup_id']
					resourceinstance_id = data['business_data']['resources'][r]['tiles'][t]['resourceinstance_id']

					nodegroup_id_comment = ''
					resourceinstance_id_comment = ''
					data_fields = []
					for node in nodes:
						if node['nodeid'] == nodegroup_id:
							nodegroup_id_comment = node['name']
						if node['nodeid'] == resourceinstance_id:
							resourceinstance_id_comment = node['name']
						for ko in data['business_data']['resources'][r]['tiles'][t]['data'].keys():
							key = str(ko)
							if node['nodeid'] == key:
								data_fields.append(node['name'])

					if len(nodegroup_id_comment) > 0:
						data['business_data']['resources'][r]['tiles'][t]['nodegroup_name'] = nodegroup_id_comment
					if len(resourceinstance_id_comment) > 0:
						data['business_data']['resources'][r]['tiles'][t]['resourceinstance_name'] = resourceinstance_id_comment
					if len(data_fields) > 0:
						data['business_data']['resources'][r]['tiles'][t]['data_fields'] = data_fields

		if options['operation'] == 'summary':

			fn = options['source']
			fp = open(fn, 'r')
			data = json.loads('\n'.join(fp.readlines()))
			fp.close()

			if not('business_data' in data):
				self.__error('', "Not a valid business data file.", str(fn))
			else:
				if not('resources' in data['business_data']):
					self.__error('', "Not a valid business data file.", str(fn))

			ret = []
			if len(self.errors) == 0:

				for item in data['business_data']['resources']:
					if not('resourceinstance' in item):
						continue
					if not('resourceinstanceid' in item['resourceinstance']):
						continue
					id = str(item['resourceinstance']['resourceinstanceid'])
					eid = self.__eamenaid_from_resourceinstance(id)
					if len(eid) == 0:
						continue
					ret.append({"uuid": id, "eamenaid": eid})

			data = ret

		if options['operation'] == 'undo':

			fn = options['source']
			fp = open(fn, 'r')
			data = json.loads('\n'.join(fp.readlines()))
			fp.close()

			if not('business_data' in data):
				self.__error('', "Not a valid business data file.", str(fn))
			else:
				if not('resources' in data['business_data']):
					self.__error('', "Not a valid business data file.", str(fn))

			uuids = []
			if len(self.errors) == 0:

				for item in data['business_data']['resources']:
					if not('resourceinstance' in item):
						continue
					if not('resourceinstanceid' in item['resourceinstance']):
						continue
					id = str(item['resourceinstance']['resourceinstanceid'])
					uuids.append(id)

			if len(uuids) > 0:
				sys.stderr.write("Attempting to delete " + str(len(uuids)) + " resources.\n")

			attempts = 0
			processed = 0
			deleted_res = 0
			deleted_tiles = 0
			deleted_indices = 0

			es = Elasticsearch(hosts=settings.ELASTICSEARCH_HOSTS)
			for id in uuids:
				try:
					ri = ResourceInstance.objects.get(resourceinstanceid=id)
				except (ValidationError, ObjectDoesNotExist):
					ri = None
				if ri is None:
					continue
				attempts = attempts + 1
				deleted_items, delete_report = ri.delete()
				if deleted_items > 0:
					processed = processed + 1
					deleted_res = deleted_res + delete_report['models.ResourceInstance']
					deleted_tiles = deleted_tiles + delete_report['models.TileModel']
					try:
						index_report = es.delete(index='eamena_resources', id=id)
					except:
						index_report = {'result': 'exception'}
					if 'result' in index_report:
						if index_report['result'] == 'deleted':
							deleted_indices = deleted_indices + 1

			if len(uuids) > 0:
				sys.stderr.write("Resources for Removal: " + str(attempts) + ", Resources Deleted: " + str(deleted_res) + ", Tiles Deleted: " + str(deleted_tiles) + ", Indices deleted: " + str(deleted_indices) + "\n")
				sys.stderr.write("Resources not found: " + str(len(uuids) - processed) + "\n")

			data = [processed, deleted_res, deleted_tiles]

		if warn_mode == 'strict':

			self.errors = self.errors + self.warnings
			self.warnings = []

		if len(self.errors) > 0:

			for error in self.errors:
				sys.stderr.write(error[1] + '\n')
				if len(error[2]) > 0:
					sys.stderr.write(error[2] + '\n')
				sys.stderr.write('\n')

			if warn_mode != 'ignore':
				for warning in self.warnings:
					sys.stderr.write(warning[1] + '\n')
					if len(warning[2]) > 0:
						sys.stderr.write(warning[2] + '\n')
					sys.stderr.write('\n')

		else:

			if warn_mode != 'ignore':
				for warning in self.warnings:
					sys.stderr.write(warning[1] + '\n')
					if len(warning[2]) > 0:
						sys.stderr.write(warning[2] + '\n')
					sys.stderr.write('\n')

			if options['dest_dir']:
				fp = open(os.path.join(options['dest_dir'], os.path.basename(options['source']) + '.json'), 'w')
				fp.write(json.dumps(data))
				fp.close()
			else:
				if options['operation'] != 'undo':
					self.stdout.write(json.dumps(data))


