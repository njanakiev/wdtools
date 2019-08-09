import os
import pickle
import logging
import requests
from simplejson import JSONDecodeError
from . import WIKIDATA_URL

logger = logging.getLogger(__name__)


def get_wikidata_label(wikidata_id, session=None, language='en'):
    session = session if session else requests.Session()
    try:
        r = session.get(WIKIDATA_URL, params={
            "action": "wbgetentities",
            "ids": wikidata_id,
            "format": "json",
            "props": "labels",
            "languages": language
        })

        entity = r.json()['entities'][wikidata_id]
        return entity.get('labels', {}).get(language, {}).get('value')
    except (requests.exceptions.RequestException, UnicodeError,
        JSONDecodeError) as e:
        logger.error("Error: %s for Wikidata id: %s", e, wikidata_id)

    return None


class WikidataLabelDictionary(object):
    def __init__(self, filepath, language='en', session=None):
        self.filepath = filepath
        self.language = language
        self.labels = {}
        self.session = session if session else requests.Session()
        self.is_updated = False

    def __enter__(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, 'rb') as f:
                self.labels = pickle.load(f)
        return self

    def __setitem__(self, wikidata_id, label):
        self.labels[wikidata_id] = label
        self.is_updated = True

    def __getitem__(self, wikidata_id):
        if wikidata_id not in self.labels:
            self.labels[wikidata_id] = get_wikidata_label(
                wikidata_id, self.session, self.language)
            self.is_updated = True
        return self.labels[wikidata_id]

    def __exit__(self, exc_type, exc_value, exc_traceback):
        with open(self.filepath, 'wb') as f:
            pickle.dump(self.labels, f)

    def save(self):
        if self.is_updated:
            with open(self.filepath, 'wb') as f:
                pickle.dump(self.labels, f)
            self.is_updated = False
