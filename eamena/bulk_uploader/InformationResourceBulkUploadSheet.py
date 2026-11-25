import json, uuid, bibtexparser

class InformationResourceBulkUploadSheet():

    def __init__(self, filename):

        self.__errors = []
        self.__data = []

        with open(filename, 'r') as fp:
            for entry in bibtexparser.parse_string(fp.read(), append_middleware=[bibtexparser.middlewares.SeparateCoAuthors()]).entries:
                item = {}
                item['uuid'] = str(uuid.uuid4())
                item['type'] = str(entry.entry_type)
                for k, v in dict(entry.fields_dict).items():
                    if k == 'title':
                        item[str(k)] = v.value.replace('{', '').replace('}', '')    
                        continue
                    if k == 'shorttitle':
                        item[str(k)] = v.value.replace('{', '').replace('}', '')    
                        continue
                    item[str(k)] = v.value
                self.__data.append(item)

    def __prune(self, data):

        if isinstance(data, (list)):
            ret = []
            for item in data:
                newitem = self.__prune(item)
                if isinstance(newitem, (bool)):
                    ret[key] = value
                else:
                    if len(newitem) > 0:
                        if newitem != 'x':
                            ret.append(newitem)
            return ret
        if isinstance(data, (dict)):
            ret = {}
            for keyob in data.keys():
                key = str(keyob)
                value = self.__prune(data[key])
                if isinstance(value, (bool)):
                    ret[key] = value
                else:
                    if len(value) > 0:
                        if value != 'x':
                            ret[key] = value
            return ret
        return data

    def count(self):

        return len(self.__data)

    def error(self, ref, error, hint):

        self.__errors.append([ref, error, hint])

    def errors(self):

        ret = []
        for item in self.__errors:
            ret.append(item)
        return ret

    def data(self, index):
        ret = {"_": self.__data[index]['uuid'], "RESOURCE_TYPE": {}, "BIBLIOGRAPHY": {"AUTHORS": []}, "URLS": []}
        for kk, v in self.__data[index].items():
            k = str(kk).lower().strip()
            if k == 'type':
                ret['RESOURCE_TYPE']['RESOURCE_TYPE'] = v
            if k == 'author':
                if not isinstance(v, list):
                    v = [v]
                for author in v:
                    ret['BIBLIOGRAPHY']['AUTHORS'].append({"AUTHOR_NAME": author})
            if k == 'title':
                ret['BIBLIOGRAPHY']['PUBLICATION_TITLE'] = {"TITLE": v}
            if k == 'editor':
                ret['BIBLIOGRAPHY']['EDITORS'] = []
                if not isinstance(v, list):
                    v = [v]
                for editor in v:
                    ret['BIBLIOGRAPHY']['EDITORS'].append(editor)
            if k == 'publisher':
                ret['BIBLIOGRAPHY']['PUBLISHER'] = v
            if k == 'volume':
                ret['BIBLIOGRAPHY']['VOLUME'] = {"VOLUME": v}
            if k == 'pages':
                ret['BIBLIOGRAPHY']['PAGES'] = {"PAGE_NUMBERS": v}
            if k == 'url':
                if len(ret['URLS']) == 0:
                    ret['URLS'].append({"URL": "", "URL_ACCESS_DATE": ""})
                ret['URLS'][0]['URL'] = v
            if k == 'urldate':
                if len(ret['URLS']) == 0:
                    ret['URLS'].append({"URL": "", "URL_ACCESS_DATE": ""})
                ret['URLS'][0]['URL_ACCESS_DATE'] = v
        return self.__prune(ret)


#[
# 'edition',
# 'title',
# 'copyright',
# 'isbn',
# 'shorttitle',
# 'url',
# 'doi',
# 'urldate',
# 'publisher',
# 'author',
# 'month',
# 'year',
# 'booktitle',
# 'editor',
# 'pages',
# 'volume',
# 'issn',
# 'language',
# 'number',
# 'journal',
# 'abstract',
# 'file',
# 'address',
# 'series',
# 'note',
# 'keywords',
# 'annote']
