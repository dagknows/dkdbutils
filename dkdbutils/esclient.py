
import sys, os, json, time, requests, random, urllib

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
    def __init__(self, current_index, esurl="http://localhost:9200", headers=None):
        if esurl.endswith("/"): esurl = esurl[:-1]
        self.esurl = esurl
        self.esreq_headers = headers or {}
        self.current_index = current_index
        self.custom_id_field = "id"
        self.maxPageSize = 9999
        self.log_timings = True
        self.log_queries = False
        self.request_maker = None

        self.validateNewDoc = None # lambda(doc_params): print "No new doc validation"
        self.applyDocPatch = None # lambda(doc, patch): print "No new doc validation on patch"

    @property
    def index_info(self):
        return self.getIndex(self.current_index)

    @property
    def elasticIndex(self):
        return f"{self.esurl}/{self.current_index}"

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
        start_time = time.time()
        if self.request_maker:
            resp = self.request_maker(method, url, payload, headers=self.esreq_headers)
        else:
            methfunc = getattr(requests, method.lower())
            if payload:
                resp = methfunc(url, json=payload, headers=self.esreq_headers)
            else:
                resp = methfunc(url, headers=self.esreq_headers)
        if self.log_timings:
            end_time = time.time()
            log(f"{method} {url}, Status: {resp.status_code}, Time Taken: {end_time - start_time} seconds")
        resp_json = resp.json()
        if "error" in resp_json and throw_if_error: raise DBException(resp_json["error"])
        return resp_json

    def get(self, docid, throw_on_missing=False):
        docid = str(docid).strip()
        safedocid = urllib.parse.quote(docid, safe='')
        path = self.elasticIndex+"/_doc/"+safedocid
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
        for t in self.listAll()["results"]:
            self.delete(t[self.custom_id_field])

    def listAll(self, page_size=None):
        return self.search(page_size=page_size)

    def count(self, query=None, accurate=False, log_queries=False):
        path = self.elasticIndex+"/_search/"
        payload = {"size": 0}
        if accurate: payload["track_total_hits"] = accurate
        if query: payload["query"] = query
        if self.log_queries or log_queries:
            log("Count Query: ", payload)
        resp = self.esrequest(path, payload=payload)
        return resp

    def deleteBy(self, query):
        path = self.elasticIndex+"/_delete_by_query?conflicts=proceed&pretty"
        return self.esrequest(path, method="POST", payload=query)

    def search(self, page_key=None, page_size=None, sort=None, query=None, filter=None, knn=None, log_queries=False):
        page_size = page_size or self.maxPageSize
        q = {
            "size": page_size,
            "seq_no_primary_term": True,
        }
        if page_key and page_key > 0:
            q["from"] = page_key
        if sort: q["sort"] = sort
        if knn:
            q["knn"] = knn
            if filter or query: q["knn"]["filter"] = filter or query
        elif filter:
            q["filter"] = filter

        if query: q["query"] = query

        path = self.elasticIndex+"/_search/"
        if self.log_queries or log_queries:
            log("Search Query: ", q)
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
        path = self.elasticIndex+"/_mget/"
        docs = self.esrequest(path, "GET", payload={
            "docs": [ {"_id": id} for id in ids ]
        })["docs"]

        out = {}
        for doc in docs:
            if "_source" in doc and doc.get("found", False):
                doc["_source"][self.custom_id_field] = doc["_id"]
                if "metadata" not in doc["_source"]:
                    doc["_source"]["metadata"] = {}
                doc["_source"]["metadata"]["_seq_no"] = doc.get("_seq_no", 0)
                doc["_source"]["metadata"]["_primary_term"] = doc.get("_primary_term", 0)
                out[doc["_id"]] = doc["_source"]
        return out

    def put(self, doc_params, refresh=""):
        if not self.validateNewDoc:
            print("self.validateNewDoc missing")
            doc = doc_params
        else:
            doc, extras = self.validateNewDoc(doc_params)

        # The main db writer
        path = self.elasticIndex+"/_doc/"
        if self.custom_id_field in doc_params:
            path += urllib.parse.quote(doc_params[self.custom_id_field], safe='')
        if refresh:
            path += f"?refresh={refresh}"
        resp = self.esrequest(path, "POST", payload=doc)
        doc[self.custom_id_field] = resp["_id"]
        return doc

    def delete(self, docid, refresh=""):
        log(f"Now deleting doc {docid}")
        safedocid = urllib.parse.quote(docid, safe='')
        path = self.elasticIndex+"/_doc/"+safedocid
        if refresh:
            path += f"?refresh={refresh}"
        resp = self.esrequest(path, "DELETE")
        return resp

    def applyPatch(self, doc_or_id, patch):
        docid, doc = self.ensureDoc(doc_or_id)
        if not self.applyDocPatch:
            print("self.applyDocPatch missing")
            doc = doc_params
        else:
            doc, extras = self.applyDocPatch(doc, patch)

    def saveOptimistically(self, doc, refresh=""):
        docid = doc[self.custom_id_field]
        safedocid = urllib.parse.quote(docid, safe='')
        doc["updated_at"] = time.time()
        seq_no = doc["metadata"]["_seq_no"]
        primary_term = doc["metadata"]["_primary_term"]
        path = f"{self.elasticIndex}/_doc/{safedocid}?if_seq_no={seq_no}&if_primary_term={primary_term}"
        if refresh:
            path += f"&refresh={refresh}"
        resp = self.esrequest(path, "POST", doc)

        # Update the version so subsequent optimistic writes can use it
        doc["metadata"]["_seq_no"] = resp["_seq_no"]
        doc["metadata"]["_primary_term"] = resp["_primary_term"]
        log("SaveDoc: ", resp)
        return doc

    def getMappings(self):
        path = self.elasticIndex
        resp = self.esrequest(path)
        return resp.get(self.current_index, {}).get("mappings", {})

    def getVersion(self):
        """ Gets the version of the index as stored in the mappings.  Returns -1 if no version found. """
        mappings = self.getMappings()
        return mappings.get("_meta", {}).get("version", -1)

    def diffIndex(self, another_index):
        """ Gets differences between the entries in the current index and of a target index.
        Returns 3 dictionaries (added, removed, changed):
            added - All entries in another that are not current index
            removed - All entries NOT in another index that are in the current index
            changed - Entries (by ID) that are in both indexes but their values differ.
        """
        added = {}
        removed = {}
        changed = {}

        curr = self.current_index
        l1 = {entry[self.custom_id_field]: entry for entry in self.listAll()["results"]}

        self.current_index = another_index
        l2 = {entry[self.custom_id_field]: entry for entry in self.listAll()["results"]}
        self.current_index = curr

        for k,v in l1.items():
            if k not in l2:
                removed[k] = v
            else:
                # Compare the two without key fields
                v2 = l2[k]
                if "metadata" in v: del v["metadata"]
                if "metadata" in v2: del v2["metadata"]
                if v != v2:
                    changed[k] = (v, v2)

        for k,v in l2.items():
            if k not in l1:
                added[k] = v


        return added, removed, changed

    def getIndex(self, index_name):
        index_url = f"{self.esurl}/{index_name}"
        resp = requests.get(index_url, headers=self.esreq_headers)
        if resp.status_code == 404:
            return None
        return resp.json().get(index_name, None)

    def deleteIndex(self, index_name):
        index_url = f"{self.esurl}/{index_name}"
        resp = requests.delete(index_url, headers=self.esreq_headers)

    def createIndex(self, new_index_name, new_index_info):
        """ Create a new index if it does not exist. """
        if self.current_index == new_index_name:
            raise Exception("Index names must be different from current_index {self.current_index}")

        new_index = self.db.getIndex(new_index_name)
        if new_index:
            # If index already exists fail
            raise Exception(f"Dest index ({new_index_name}) already exists")
        else:
            new_index = self.putIndex(new_index_name, new_index_info)
            if not new_index:
                raise Exception(f"Dest index ({new_index_name}) could not be created")
        return new_index

    def putIndex(self, index_name, index_info):
        """ putIndex creates a new index.  If index already exists, this call will fail """
        index_url = f"{self.esurl}/{index_name}"
        resp = requests.put(index_url, json=index_info, headers=self.esreq_headers)
        print(f"Created new index ({index_url}): ", resp.status_code, resp.content)
        if resp.status_code == 200 and resp.json()["acknowledged"]:
            return self.getIndex(index_name)
        raise Exception("Failed to create index: ", resp.json())

    def listIndexes(self):
        return requests.get(self.esurl + "/_aliases", headers=self.esreq_headers).json()

    def reindexTo(self, dst_index_name, onconflicts="proceed", fixfunc=None):
        """ Indexes the current index into another index. """
        src = self.current_index
        reindex_json = {
            "source" : { "index" : src },
            "dest" : { "index" : dst_index_name },
            "conflicts": onconflicts,
        }
        print(f"Reindexing from {src} -> {dst_index_name}...")
        resp = requests.post(self.esurl + '/_reindex?refresh=true', json=reindex_json, headers=self.esreq_headers)
        respjson = resp.json()
        print(f"Reindex ({src} -> {dst_index_name}) response: ", reindex_json, resp.status_code, resp.content)
        failures = respjson.get("failures", [])
        if failures:
            print(f"Reindex Response: ", respjson)
            srcindex = DB(self.esurl, current_index=src)
            dstindex = DB(self.esurl, current_index=dst)
            for failed in failures:
                print("Failed to reindex item: ", failed)
                if fixfunc: fixfunc(failed, srcindex, dstindex)
        return respjson


    def aliasIndex(self, alias_name):
        log("Trying to add alias: ", alias_name, " to index: ", self.current_index)
        alias_json = {
            "actions" : [
                { "add" : { "index" : self.current_index, "alias" : alias_name } }
            ]
        }
        resp_alias = requests.post(self.esurl + "/_aliases", json=alias_json, headers=self.esreq_headers)

    def migrateToIndex(self, index_name, index_info):
        """ Migrates data to another (NEW) index by creating it.  """
        to_index = self.createIndex(index_name, index_info)
        response = self.reindexTo(index_name)
        return to_index

    def _copy_between(self, src_index_name, dst_index_name, on_conflict, index_info):
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
        resp = requests.get(index_url, headers=self.esreq_headers)
        if resp.status_code == 404:
            print("Creating new index for org: ", index_url, org, file=sys.stdout)
            return self.putIndex(index_url, index_table, version)

def testit():
    import ipdb ; ipdb.set_trace()
    db = DB("mydoc")
    db.getIndex("v1")
