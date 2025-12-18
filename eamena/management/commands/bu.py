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
			data = convert(options['graph'], options['source'], options['bus_language'], options['warn_mode'], (options['append_mode'] == 'append'))

		if options['operation'] == 'validate':
			data = validate(options['graph'], options['source'], options['bus_language'], options['warn_mode'], (options['append_mode'] == 'append'))

		if options['operation'] == 'prerequisites':
			data = prerequisites(options['graph'], options['source'], options['bus_language'], options['warn_mode'], (options['append_mode'] == 'append'))

		if options['operation'] == 'unflatten':
			data = unflatten(options['graph'], options['source'], options['bus_language'], options['warn_mode'], (options['append_mode'] == 'append'))

		if options['operation'] == 'translate':
			data = translate(options['graph'], options['source'], options['bus_language'], options['warn_mode'], (options['append_mode'] == 'append'))

		if options['operation'] == 'list_nodes':
			data = list_nodes(options['graph'], options['bu_language'], options['warn_mode'])

		if options['operation'] == 'annotate':
			data = annotate(options['graph'], options['source'], options['bus_language'], options['warn_mode'])

		if options['operation'] == 'summary':
			data = summary(options['source'], options['bus_language'])

		if options['operation'] == 'undo':
			data = undo(options['source'])

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


