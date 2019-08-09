import os
import re
import pickle
import logging
import requests
import pandas as pd
from . import query, labels

logger = logging.getLogger(__name__)


def split_bbox(bbox, n=1, buffer_pct=0.01):
    t = [v / n for v in range(n + 1)]
    buffer = buffer_pct * (bbox[2] - bbox[0]) / n
    for i in range(n):
        for j in range(n):
            x0 = bbox[0] + t[i] * (bbox[2] - bbox[0]) - buffer
            y0 = bbox[1] + t[j] * (bbox[3] - bbox[1]) - buffer
            x1 = bbox[0] + t[i + 1] * (bbox[2] - bbox[0]) + buffer
            y1 = bbox[1] + t[j + 1] * (bbox[3] - bbox[1]) + buffer
            yield [x0, y0, x1, y1]


def instanceof_wikidata_ids(instanceof,
    filepath, overwrite=False, chunksize=None):

    if not os.path.exists(filepath) or overwrite:
        df = query.wikidata_query("""
            SELECT ?wikidata_id WHERE {{
                ?wikidata_id wdt:P31/wdt:P279* wd:{}.
            }}""".format(instanceof))
        df['wikidata_id'] = df['wikidata_id'].str.split(
            "http://www.wikidata.org/entity/").str[1]
        df.to_csv(filepath, index=False, header=False)

    return pd.read_csv(filepath, names=['wikidata_id'], chunksize=chunksize)


def wikidata_bbox(bbox):
    df = query.wikidata_query("""
        SELECT ?wikidata_id ?instance_of_id ?geom
        WHERE {{
          ?wikidata_id wdt:P31 ?instance_of_id.
          SERVICE wikibase:box {{
            ?wikidata_id wdt:P625 ?geom .
            bd:serviceParam wikibase:cornerWest
              "Point({} {})"^^geo:wktLiteral.
            bd:serviceParam wikibase:cornerEast
              "Point({} {})"^^geo:wktLiteral.
          }}
        }}""".format(*bbox))
    df['wikidata_id'] = df['wikidata_id'].str.split(
        "http://www.wikidata.org/entity/").str[1]
    df['instance_of_id'] = df['instance_of_id'].str.split(
        "http://www.wikidata.org/entity/").str[1]
    return df


def wikidata_bbox_to_file(bbox, filepath, n_splits,
    labels_filepath, compression='infer', language='en'):
    with labels.WikidataLabelDictionary(
        labels_filepath, language) as wikidata_labels:

        for i, bbox_subset in enumerate(split_bbox(bbox, n_splits, 0.0)):
            logger.debug("Bounding box index %d / %d",
                i + 1, n_splits * n_splits)
            df = wikidata_bbox(bbox_subset)

            # Add instance_of label
            df['instance_of'] = df['instance_of_id'].apply(
                lambda idx: wikidata_labels[idx]
                    if isinstance(idx, str) and re.match(r'Q[0-9]+$', idx)
                    else None)

            if i == 0:
                df.to_csv(filepath, compression=compression,
                    index=False)
            else:
                df.to_csv(filepath, compression=compression,
                    index=False, mode='a', header=False)

            wikidata_labels.save()


def wikidata_items(df, folderpath, language='en', overwrite=False):
    logger.debug('collect_wikidata_items started')
    session = requests.Session()

    start_time = time.time()
    if isinstance(df, pd.io.parsers.TextFileReader):
        iterator = enumerate(df)
    else:
        iterator = df.iterrows()

    for i, chunk in iterator:
        wikidata_ids = chunk['wikidata_id']
        logger.debug("Chunk %d", i)
        if isinstance(wikidata_ids, pd.core.series.Series):
            if wikidata_ids.apply(lambda entity_id: os.path.exists(
                os.path.join(folderpath, entity_id))).all():
                continue
        else:
            filepath = os.path.join(folderpath, wikidata_ids)
            if os.path.exists(filepath):
                continue

        data = query.get_wikidata_entity(
            wikidata_ids, session, language)

        if data is None:
            continue

        for entity_id in data['entities']:
            filepath = os.path.join(folderpath, entity_id)
            entity = data['entities'].get(entity_id)
            if not os.path.exists(filepath) or overwrite:
                if entity is not None:
                    with open(filepath, 'wb') as f:
                        pickle.dump(entity, f)
                        logger.debug("Content of Wikidata id: %s saved in %s",
                                      entity_id, filepath)

    logger.debug('Total duration: %f', time.time() - start_time)
