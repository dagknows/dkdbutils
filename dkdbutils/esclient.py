
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
    def __init__(self, current_index, esurl="http://localhost:9200", index_suffix=""):
        self.esurl = esurl
        self.current_index = current_index
        self.index_suffix = index_suffix
        self.custom_id_field = "id"
        self.maxPageSize = 9999

        self.validateNewDoc = None # lambda(doc_params): print "No new doc validation"
        self.applyDocPatch = None # lambda(doc, patch): print "No new doc validation on patch"

    @property
    def index_info(self):
        return self.getIndex(self.fullIndexName)

    @property
    def fullIndexName(self):
        return f"{self.current_index}{self.index_suffix}"

    @property
    def elasticIndex(self):
        out = f"{self.esurl}/{self.current_index}{self.index_suffix}"
        print("Index: ", out)
        return out

    def ensureDoc(self, doc_or_id):
        if type(doc_or_id) is str:
            docid = doc_or_id
            doc = self.get(docid.strip())
        else:
            doc = doc_or_id
            docid = doc[self.custom_id_field]
        if not doc: raise UserException(f"Doc not found {docid}")
        return doc[self.custom_id_field], doc

    def esrequest(self, url, method="GET", payload=None, throw_if_error=True):
        methfunc = geteattr(requests, method.lower())
        if payload:
            resp = methfunc(url, json=payload).json()
        else:
            resp = methfunc(url, json=payload).json()
        if "error" in resp and throw_if_error: raise DBException(resp["error"])
        return resp

    def get(self, docid, throw_on_missing=False):
        docid = str(docid).strip()
        path = self.elasticIndex+"/_doc/"+docid
        resp = self.esrequest(path)
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
        for t in self.list()["results"]:
            self.delete(t[self.custom_id_field])

    def search(self, query):
        query = query or {}
        if "size" not in query: query["size"] = self.maxPageSize
        query["seq_no_primary_term"] = True
        path = self.elasticIndex+"/_search/"
        resp = self.esrequest.get(path, payload=query)
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

    def listAll(self, page_size=None):
        return self.search(page_size=page_size)

    def search(self, page_key=None, page_size=None, sort=None, query=None):
        page_size = page_size or self.maxPageSize
        q = {
            "size": page_size,
            "seq_no_primary_term": True,
        }
        if sort: q["sort"] = sort
        if query: q["query"] = query
        path = self.elasticIndex+"/_search/"
        resp = self.esrequest(path, payload=q)
        if "hits" not in resp: return {"results": []}
        hits = resp["hits"]
        if "hits" not in hits: return {"results": []}
        hits = hits["hits"]
        for h in hits:
            h["_source"][self.custom_id_field] = h["_id"]
            if "metadata" not in h["_source"]:
                h["_source"]["metadata"] = {}
            h["_source"]["metadata"]["_seq_no"] = h.get("_seq_no", 0)
            h["_source"]["metadata"]["_primary_term"] = h.get("_primary_term", 0)
        return {"results": [h["_source"] for h in hits]}


    def batchGet(self, ids):
        # TODO - is there a batch get or id "IN" query in elastic?
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-ids-query.html
        query = { "query": { "ids" : { "values" : ids } } }
        results = self.search(query=query)
        return results

    def put(self, doc_params):
        if not self.validateNewDoc:
            print("self.validateNewDoc missing")
            doc = doc_params
        else:
            doc, extras = self.validateNewDoc(doc_params)

        # The main db writer
        path = self.elasticIndex+"/_doc/"
        if self.custom_id_field in doc_params:
            path += doc_params[self.custom_id_field]
        resp = self.esrequest(path, "POST", payload=q)
        log("Created Doc: ", resp)
        if "error" in resp:
            raise DBException(resp["error"])
        doc[self.custom_id_field] = resp["_id"]
        return doc

    def delete(self, doc_or_id):
        docid, doc = self.ensureDoc(doc_or_id)

        log(f"Now deleting doc {docid}")
        path = self.elasticIndex+"/_doc/"+docid
        resp = self.esrequest(path, "DELETE")
        return resp

    def applyPatch(self, doc_or_id, patch):
        docid, doc = self.ensureDoc(doc_or_id)
        if not self.applyDocPatch:
            print("self.applyDocPatch missing")
            doc = doc_params
        else:
            doc, extras = self.applyDocPatch(doc, patch)

    def saveOptimistically(self, doc):
        tid = doc[self.custom_id_field]
        doc["updated_at"] = time.time()
        seq_no = doc["metadata"]["_seq_no"]
        primary_term = doc["metadata"]["_primary_term"]
        path = f"{self.elasticIndex}/_doc/{tid}?if_seq_no={seq_no}&if_primary_term={primary_term}"
        resp = self.esrequest(path, "POST", doc)

        # Update the version so subsequent optimistic writes can use it
        doc["metadata"]["_seq_no"] = resp["_seq_no"]
        doc["metadata"]["_primary_term"] = resp["_primary_term"]
        log("SaveDoc: ", resp)
        return doc

    def getMappings(self):
        path = self.elasticIndex
        resp = self.esrequest(path)
        return resp.get(self.fullIndexName, {}).get("mappings", {})

    def getVersion(self):
        """ Gets the version of the index as stored in the mappings.  Returns -1 if no version found. """
        mappings = self.getMappings()
        return mappings.get("_meta", {}).get("version", -1)

    def copy_between(self, src_index_name, dst_index_name, on_conflict, index_info):
        """ Reindexing is a huuuuuuuge pain.  This method is meant to be "generic" and growing over time and be as "forgiving" as possible (at the expense of speed)

        The following things are done:

        1. Check the current version of an index mapping
            * Could be "empty" as the user never created a "default" index
            * Could be in an incompatible state
            * both above are really kinda same because ES defaults most fields to "TEXT"

        2a. If dest index does not exist - create with the new mappings
        2b. If dest index exists then ensure its mapping match the new mapping (fail if not equal)
            * Checking for mappings matching is just by checking "version numbers" as a full deep check 
              may be flaky (as elastic may itself add extra attribs to a mapping)

        3. While no conflicts:
                a reindex from src -> dest indexes
                b for conflicting docs:
                    * apply the on_conflict method
                    * [Not sure if needed] - Remove all items from dest table as you may not be able to an index with entries
                c goto (a)

        4. Mark dst_index as "_alias"
        """
        src_index = self.getIndex(src_index_name)
        if src_index is None:
            # Doesnt exist so just create dest and get out
            return self.putIndex(dest_index, index_info)

        dest_index = self.getIndex(dest_index_name)
        if dest_index is None:
            # Doesnt exist so just create dest and get out
            dest_index = self.putIndex(dest_index, index_info)

        print("EnsuringIndex for: ", org, index_name, index_url, version)
        resp = requests.get(index_url)
        if resp.status_code == 404:
            print("Creating new index for org: ", index_url, org, file=sys.stdout)
            return self.putIndex(index_url, index_table, version)

    def getIndex(self, index_name):
        index_url = f"{self.esurl}/{index_name}"
        resp = requests.get(index_url)
        if resp.status_code == 404:
            return None
        return resp.json()[index_name]

    def deleteIndex(self, index_name):
        index_url = f"{self.esurl}/{index_name}"
        resp = requests.delete(index_url)

    def putIndex(self, index_name, index_info):
        """ putIndex creates a new index.  If index already exists, this call will fail """
        index_url = f"{self.esurl}/{index_name}"
        resp = requests.put(index_url, json=index_info)
        print(f"Created new index ({index_url}): ", resp.status_code, resp.content)
        if resp.status_code == 200 and resp.json()["acknowledged"]:
            return self.getIndex(index_name)
        return None

    def listIndexes(self):
        return requests.get(self.esurl + "/_aliases").json()

    def reindexTo(self, dst):
        """ Indexes the current index into another index. """
        src = self.fullIndexName
        reindex_json = {
            "source" : { "index" : src },
            "dest" : { "index" : dst },
            "conflicts": "proceed",
        }
        resp = requests.post(self.esurl + '/_reindex?refresh=true', json=reindex_json)

        respjson = resp.json()
        """
        failures = respjson.get("failures", [])
        dstdb = DB(dst)
        for failed_doc in failures:
            docid, doc = self.ensureDoc(failed_doc["id"])
            resolved = on_conflict(doc)
            print("Remapping {src}.{docid} -> {dst}.{docid}: ")
            print("     Conflict Doc: ", doc)
            dstdb.put(resolved)
            print("     Resolved Doc: ", resolved)
        """

        print(f"Reindex ({src} -> {dst}) response: ", reindex_json, resp.status_code, resp.content)
        return respjson


def testit():
    import ipdb ; ipdb.set_trace()
    db = DB("mydoc")
    db.getIndex("v1")
