import os
import re
import pickle
import logging
import requests
import pandas as pd
import wdtools

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
        df = wdtools.query.wikidata_query("""
            SELECT ?wikidata_id WHERE {{
                ?wikidata_id wdt:P31/wdt:P279* wd:{}.
            }}""".format(instanceof))
        df['wikidata_id'] = df['wikidata_id'].str.split(
            "http://www.wikidata.org/entity/").str[1]
        df.to_csv(filepath, index=False, header=False)

    return pd.read_csv(filepath, names=['wikidata_id'], chunksize=chunksize)


def wikidata_bbox(bbox, prop=None, prop_id=None):
    if prop is None:
        prop = 'property'
        query = f"""
            SELECT ?wikidata_id ?property_id ?geom
            WHERE {{
              ?wikidata_id ?p ?statement.
              ?property_id wikibase:claim ?p.
              SERVICE wikibase:box {{
                ?wikidata_id wdt:P625 ?geom .
                bd:serviceParam wikibase:cornerWest
                  "Point({bbox[0]} {bbox[1]})"^^geo:wktLiteral.
                bd:serviceParam wikibase:cornerEast
                  "Point({bbox[2]} {bbox[3]})"^^geo:wktLiteral.
              }}
            }}"""
    else:
        query = f"""
            SELECT ?wikidata_id ?{prop}_id ?geom
            WHERE {{
              ?wikidata_id wdt:{prop_id} ?{prop}_id.
              SERVICE wikibase:box {{
                ?wikidata_id wdt:P625 ?geom .
                bd:serviceParam wikibase:cornerWest
                  "Point({bbox[0]} {bbox[1]})"^^geo:wktLiteral.
                bd:serviceParam wikibase:cornerEast
                  "Point({bbox[2]} {bbox[3]})"^^geo:wktLiteral.
              }}
            }}"""

    df = wdtools.query.wikidata_query(query).drop_duplicates()
    logger.debug("Table shape: %s", str(df.shape))

    df['wikidata_id'] = df['wikidata_id'].str.split(
        "http://www.wikidata.org/entity/").str[1]
    df[prop + '_id'] = df[prop + '_id'].str.split(
        "http://www.wikidata.org/entity/").str[1]
    return df


def wikidata_bbox_to_file(bbox, filepath, n_splits,
    labels_filepath, prop=None, prop_id=None,
    compression='infer', language='en'):

    property_name = prop if prop else "property"

    with wdtools.labels.WikidataLabelDictionary(
        labels_filepath, language) as wikidata_labels:

        for i, bbox_subset in enumerate(split_bbox(bbox, n_splits, 0.0)):
            logger.debug("Bounding box index %d / %d, bbox: %s",
                i + 1, n_splits * n_splits, str(bbox_subset))
            df = wikidata_bbox(bbox_subset, prop, prop_id)

            # Add instance_of label
            df[property_name] = df[property_name + '_id'].apply(
                lambda idx: wikidata_labels[idx]
                    if isinstance(idx, str) and re.match(r'[QP][0-9]+$', idx)
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

        data = wdtools.query.get_wikidata_entity(
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
