import json, os
from tempfile import NamedTemporaryFile

from django.core.exceptions import ValidationError
from django.db import connection
from django.http import HttpRequest
from django.utils.translation import gettext as _
from django.core.files.storage import default_storage
from django.contrib.auth.models import User
from arches.app.datatypes.datatypes import DataTypeFactory
from arches.app.etl_modules.decorators import load_data_async
from arches.app.models.models import Node, TileModel, ETLModule
from arches.app.models.system_settings import settings
from arches.app.etl_modules.base_import_module import (
    BaseImportModule,
    FileValidationError,
)
import eamena.tasks as tasks

details = {
    "etlmoduleid": "9b48b02b-0a45-4b4c-96d3-9e780ea3d2ff",
    "name": "Heritage Place Import",
    "description": "EAMENA Bulk Upload Sheet",
    "etl_type": "import",
    "component": "views/components/etl_modules/heritage-place-import",
    "componentname": "heritage-place-import",
    "modulename": "heritage_place_import.py",
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

    def run_load_task(self, userid, files, summary, result, temp_dir, loadid, multiprocessing=False):
        
        self.loadid = request.POST.get("load_id")
        self.temp_dir = os.path.join(settings.UPLOADED_FILES_DIR, "tmp", self.loadid)
        self.file_details = request.POST.get("load_details", None)
        result = {}
        if self.file_details:
            details = json.loads(self.file_details)
            files = details["result"]["summary"]["files"]
            summary = details["result"]["summary"]

        load_task = tasks.load_etl_file.apply_async(
            (self.userid, files, summary, result, self.temp_dir, self.loadid),
        )
        with connection.cursor() as cursor:
            cursor.execute(
                """UPDATE load_event SET taskid = %s WHERE loadid = %s""",
                (load_task.task_id, self.loadid),
            )

    def validate_uploaded_file(self, workbook):
        pass # We accept all XLSX files at this point.

