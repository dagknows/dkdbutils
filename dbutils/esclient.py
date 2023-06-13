
import sys, os, json, time, requests, random

def log(*args, **kwargs):
    print(*args, **kwargs)
    sys.stdout.flush()

class DBException(Exception): pass
class UserException(Exception): pass

def randomStringDigits(stringLength=16):
    import string, random
    lettersAndDigits = string.ascii_letters + string.digits
    x = random.choice(string.ascii_letters)
    x += ''.join(random.choice(lettersAndDigits) for i in range(stringLength-1))
    return x

class DB(object):
    def __init__(self, current_index, esurl="http://localhost:9200", index_version="_alias"):
        self.esurl = esurl
        self.current_index = current_index
        self.index_version = index_version
        self.custom_id_field = "id"
        self.maxPageSize = 9999

        self.validateNewDoc = None # lambda(doc_params): print "No new doc validation"
        self.applyDocPatch = None # lambda(doc, patch): print "No new doc validation on patch"

    @property
    def elasticIndex(self):
        out = f"{self.esurl}/{self.current_index}{self.index_version}"
        print("Index: ", out)
        return out

    def ensureDoc(self, doc_or_id):
        if type(doc_or_id) is str:
            docid = doc_or_id
            doc = self.getDocById(docid.strip())
        else:
            doc = doc_or_id
            docid = doc[self.custom_id_field]
        if not doc: raise UserException(f"Doc not found {docid}")
        return doc[self.custom_id_field], doc

    def getDocById(self, docid, throw_on_missing=False):
        docid = str(docid).strip()
        path = self.elasticIndex+"/_doc/"+docid
        resp = requests.get(path).json()
        if "error" in resp: raise DBException(resp["error"])
        if not resp["found"] or "_source" not in resp:
            if throw_on_missing:
                raise UserException(f"Invalid docid: {docid}")
            return None
        out = resp["_source"]
        out[self.custom_id_field] = resp["_id"]
        if "metadata" not in out:
            out["metadata"] = {}
        out["metadata"]["_seq_no"] = resp["_seq_no"]
        out["metadata"]["_primary_term"] = resp["_primary_term"]
        return out

    def deleteAll(self):
        for t in self.listDocs()["results"]:
            self.deleteDoc(t[self.custom_id_field])

    def searchDocs(self, query):
        query = query or {}
        if "size" not in query: query["size"] = self.maxPageSize
        query["seq_no_primary_term"] = True
        path = self.elasticIndex+"/_search/"
        resp = requests.get(path, json=query).json()
        if "hits" not in resp: return []
        hits = resp["hits"]
        if "hits" not in hits: return []
        hits = hits["hits"]
        for h in hits:
            h["_source"][self.custom_id_field] = h["_id"]
            if "metadata" not in h["_source"]:
                h["_source"]["metadata"] = {}
            h["_source"]["metadata"]["_seq_no"] = h.get("_seq_no", 0)
            h["_source"]["metadata"]["_primary_term"] = h.get("_primary_term", 0)
        return {"results": [h["_source"] for h in hits]}

    def listDocs(self, sort=None, query=None):
        # TODO - pagination
        query = {}
        if sort: query["sort"] = sort
        if query: query["query"] = query
        return self.searchDocs(query)

    def batchGet(self, ids):
        # TODO - is there a batch get or id "IN" query in elastic?
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-ids-query.html
        query = { "query": { "ids" : { "values" : ids } } }
        results = self.searchDocs(query=query)
        return results

    def createDoc(self, doc_params):
        if not self.validateNewDoc:
            print("self.validateNewDoc missing")
            doc = doc_params
        else:
            doc, extras = self.validateNewDoc(doc_params)

        # The main db writer
        path = self.elasticIndex+"/_doc/"
        resp = requests.post(path, json=doc).json()
        log("Created Doc: ", resp)
        if "error" in resp:
            raise DBException(resp["error"])
        doc[self.custom_id_field] = resp["_id"]
        return doc

    def deleteDoc(self, doc_or_id):
        docid, doc = self.ensureDoc(doc_or_id)

        log(f"Now deleting doc {docid}")
        path = self.elasticIndex+"/_doc/"+docid
        resp = requests.delete(path)
        return resp.json()

    def applyPatch(self, doc_or_id, patch):
        docid, doc = self.ensureDoc(doc_or_id)
        if not self.applyDocPatch:
            print("self.applyDocPatch missing")
            doc = doc_params
        else:
            doc, extras = self.applyDocPatch(doc, patch)

    def searchDocs(self, query):
        query = query or {}
        if "size" not in query: query["size"] = 9999
        query["seq_no_primary_term"] = True
        path = self.elasticIndex+"/_search/"
        resp = requests.get(path, json=query).json()
        if "hits" not in resp: return []
        hits = resp["hits"]
        if "hits" not in hits: return []
        hits = hits["hits"]
        for h in hits:
            h["_source"][self.custom_id_field] = h["_id"]
            if "metadata" not in h["_source"]:
                h["_source"]["metadata"] = {}
            h["_source"]["metadata"]["_seq_no"] = h.get("_seq_no", 0)
            h["_source"]["metadata"]["_primary_term"] = h.get("_primary_term", 0)
        return {"results": [h["_source"] for h in hits]}

    def saveDocOptimistically(self, doc):
        tid = doc[self.custom_id_field]
        doc["updated_at"] = time.time()
        seq_no = doc["metadata"]["_seq_no"]
        primary_term = doc["metadata"]["_primary_term"]
        path = f"{self.elasticIndex}/_doc/{tid}?if_seq_no={seq_no}&if_primary_term={primary_term}"
        resp = requests.post(path, json=doc).json()
        if "error" in resp:
            log("SaveDoc Error: ", resp["error"])
            raise DBException(resp["error"])
        else:
            log("SaveDoc: ", resp)
        return doc
