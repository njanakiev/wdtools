import requests
import logging
import pandas as pd
from simplejson import JSONDecodeError
from . import WIKIDATA_URL, WIKIDATA_SPARQL_URL

logger = logging.getLogger(__name__)


def wikidata_query(sparql_query, headers=None, session=None):
    session = session if session else requests.Session()
    try:
        params = { 'format': 'json', 'query': sparql_query }
        r = session.get(WIKIDATA_SPARQL_URL, params=params, headers=headers)
        data = r.json()
    except JSONDecodeError as e:
        print(r.content)
        raise Exception('Invalid query')

    if ('results' in data) and ('bindings' in data['results']):
        columns = data['head']['vars']
        rows = [[binding[col]['value'] if col in binding else None
                for col in columns]
                for binding in data['results']['bindings']]
    else:
        raise Exception('No results')

    return pd.DataFrame(rows, columns=columns)


def get_wikidata_entity(wikidata_ids, session=None, language='en',
    props="claims|labels", num_retries=100):
    session = session if session else requests.Session()

    if not isinstance(wikidata_ids, str):
        wikidata_ids = '|'.join(wikidata_ids)

    try:
        for retry in range(num_retries):
            params = {
                "action": "wbgetentities",
                "ids": wikidata_ids,
                "format": "json",
                "languages": language,
                "props": props
            }
            r = session.get(WIKIDATA_URL, params=params)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                logger.warn('429 Too Many Requests for Wikidata id: %s',
                    wikidata_id)
                time.sleep(1)
            elif r.status_code == 503:
                logger.warn('503 Service Unavailable for Wikidata id: %s',
                    wikidata_id)
                time.sleep(1)
            else:
                raise Exception("Response code %d" % r.status_code)

    except (requests.exceptions.RequestException, UnicodeError,
        JSONDecodeError) as e:
        logger.error("Error: %s for Wikidata id: %s", e, wikidata_ids)

    return None
