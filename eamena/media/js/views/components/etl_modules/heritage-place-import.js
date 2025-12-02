define([
    'underscore',
    'knockout',
    'viewmodels/base-import-view-model',
    'arches',
    'viewmodels/alert',
    'viewmodels/excel-file-import',
    'templates/views/components/etl_modules/heritage-place-import.htm',
    'dropzone',
    'bindings/select2-query',
    'bindings/dropzone',
], function(_, ko, ImporterViewModel, arches, AlertViewModel, ExcelFileImportViewModel, hpImporterTemplate) {
    return ko.components.register('heritage-place-import', {
        viewModel: ExcelFileImportViewModel,
        template: hpImporterTemplate,
    });
});