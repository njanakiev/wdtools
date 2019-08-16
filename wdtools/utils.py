import logging
import sqlalchemy
import shapely.wkt
import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement


logger = logging.getLogger(__name__)


def csv_to_postgis(connection_uri, filepath, table_name, chunksize=200000):
    for i, df in enumerate(pd.read_csv(filepath, compression='gzip',
        chunksize=chunksize)):
        logger.debug("Running chunk: %d", i + 1)

        # Convert WKT to shapely object
        df['geom'] = df['geom'].apply(shapely.wkt.loads)

        # Convert Pandas dataframe to GeoPandas dataframe
        gdf = gpd.GeoDataFrame(df, geometry='geom', crs={'init': 'epsg:4326'})

        # Creating SQLAlchemy's engine to use
        engine = sqlalchemy.create_engine(connection_uri)

        gdf['geom'] = gdf['geom'].apply(lambda x: WKTElement(x.wkt, srid=4326))

        # Use 'dtype' to specify column's type
        # For the geom column, we will use GeoAlchemy's type 'Geometry'
        if_exists = 'replace' if i == 0 else 'append'
        gdf.to_sql(table_name, engine, if_exists=if_exists, index=False,
                   dtype={'geom': Geometry('POINT', srid=4326)})
