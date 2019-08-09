import os
import pickle
import logging
import requests
import numpy as np
import pandas as pd
from . import query, labels

logger = logging.getLogger(__name__)


def split_bbox(bbox, n=1, buffer_pct=0.01):
    t = np.linspace(0, 1, n + 1)
    buffer = buffer_pct * (bbox[2] - bbox[0]) / n
    for i in range(n):
        for j in range(n):
            x0 = bbox[0] + t[i] * (bbox[2] - bbox[0]) - buffer
            y0 = bbox[1] + t[i] * (bbox[3] - bbox[1]) - buffer
            x1 = bbox[0] + t[i + 1] * (bbox[2] - bbox[0]) + buffer
            y1 = bbox[1] + t[i + 1] * (bbox[3] - bbox[1]) + buffer
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


def wikidata_bbox_to_file(bbox, filepath, n_splits, compression='infer'):
    with labels.WikidataLabelDictionary(
        labels_filepath, language) as wikidata_labels:

        for i, bbox_subset in enumerate(split_bbox(bbox, n_splits, 0.0)):
            logger.debug("Bounding box index %d / %d",
                i + 1, n_splits * n_splits)
            df = wikidata_bbox(bbox_subset)

            # Add instance_of label
            df['instance_of'] = df['instance_of_id'].apply(
                lambda idx: wikidata_labels[idx])

            if i == 0:
                df.to_csv(filepath, compression=compression,
                    mode='a', index=False)
            else:
                df.to_csv(filepath, compression=compression,
                    mode='a', index=False, header=False)
