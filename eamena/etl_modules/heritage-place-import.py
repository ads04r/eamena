from datetime import datetime
import json
from openpyxl import load_workbook
import os
from tempfile import NamedTemporaryFile

from django.core.exceptions import ValidationError
import uuid
from django.db import connection
from django.http import HttpRequest, HttpResponse
from django.utils.translation import gettext as _
from django.core.files.storage import default_storage
from django.contrib.auth.models import User
from arches.app.datatypes.datatypes import DataTypeFactory
from arches.app.etl_modules.decorators import load_data_async
from arches.app.models.models import Node, TileModel, ETLModule
from arches.app.models.system_settings import settings
from arches.app.utils.betterJSONSerializer import JSONSerializer
from arches.app.etl_modules.base_import_module import (
    BaseImportModule,
    FileValidationError,
)
import arches.app.tasks as tasks
from arches.management.commands.etl_template import create_tile_excel_workbook

details = {
    "etlmoduleid": "9b48b02b-0a45-4b4c-96d3-9e780ea3d2ff",
    "name": "Heritage Place Import",
    "description": "EAMENA Bulk Upload Sheet",
    "etl_type": "import",
    "component": "views/components/etl_modules/heritage-place-import",
    "componentname": "heritage-place-import",
    "modulename": "heritage-place-import.py",
    "classname": "HeritagePlaceImporter",
    "config": {"bgColor": "#42485a", "circleColor": "#7f7f7f"},
    "icon": "fa fa-institution",
    "slug": "heritage-place-import",
    "helpsortorder": 9,
    "helptemplate": "heritage-place-import-help"
}

class HeritagePlaceImporter(BaseImportModule):
    def __init__(self, request=None, loadid=None, temp_dir=None, params=None):
        self.loadid = request.POST.get("load_id") if request else loadid
        self.userid = (
            request.user.id
            if request
            else settings.DEFAULT_RESOURCE_IMPORT_USER["userid"]
        )
        self.mode = "cli" if not request and params else "ui"
        try:
            self.user = User.objects.get(pk=self.userid)
        except User.DoesNotExist:
            raise User.DoesNotExist(
                _(
                    "The userid {} does not exist. Probably DEFAULT_RESOURCE_IMPORT_USER is not configured correctly in settings.py.".format(
                        self.userid
                    )
                )
            )
        if not request and params:
            request = HttpRequest()
            request.user = self.user
            request.method = "POST"
            for k, v in params.items():
                request.POST.__setitem__(k, v)
        self.request = request
        self.moduleid = request.POST.get("module") if request else None
        self.datatype_factory = DataTypeFactory()
        self.legacyid_lookup = {}
        self.temp_path = ""
        self.temp_dir = temp_dir if temp_dir else None
        self.config = (
            ETLModule.objects.get(pk=self.moduleid).config if self.moduleid else {}
        )

    def validate(self, request):
        pass

    def validate_inputs(self, request):
        pass

    def edit_staged_data(self, cursor, graph_id, node_id, operation, language_code, pattern, new_text):
        pass

    def get_preview_data(self, node_id, search_url, language_code, operation, old_text, case_insensitive, whole_word):
        pass

    def preview(self, request):
        pass

    def write(self, request):
        # This function is called first
        self.temp_dir = os.path.join(settings.UPLOADED_FILES_DIR, "tmp", self.loadid)
        self.file_details = request.POST.get("load_details", None)
        multiprocessing = request.POST.get("multiprocessing", False)
        result = {}
        if self.file_details:
            details = json.loads(self.file_details)
            files = details["result"]["summary"]["files"]
            summary = details["result"]["summary"]
            use_celery_file_size_threshold = self.config.get(
                "celeryByteSizeLimit", 100000
            )

            if (
                self.mode != "cli"
                and summary["cumulative_files_size"] > use_celery_file_size_threshold
            ):
                response = self.run_load_task_async(request, self.loadid)
            else:
                response = self.run_load_task(
                    self.userid,
                    files,
                    summary,
                    result,
                    self.temp_dir,
                    self.loadid,
                    multiprocessing,
                )

            return response

    @load_data_async
    def run_load_task_async(self, request):
        pass

    def run_load_task(self, userid, loadid, module_id, graph_id, node_id, operation, language_code, pattern, new_text, resourceids):
        pass
