from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData, Table
from geoalchemy2.functions import ST_Intersects
from contextlib import contextmanager
from src.d00_utils.gbounds import bbox_to_wkt
from src.d00_utils.postgis_helpers import *
from src.d00_utils.loggers import getalogger




# Define base for models
Base = declarative_base()
logger = getalogger("postgis_select", 10)


# Define the spatial table class
class PostGIS_Table(Base):

    __tablename__ = 'my_spatial_table'
    geom = Column(Geometry('POLYGON', srid=4326))  # Default geom column if not dynamically loaded

    @classmethod
    def from_table(cls, engine, table_name='my_spatial_table'):
        """Dynamically reflects the table schema and includes all columns."""
        # Create metadata and reflect the table
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine)

        # Dynamically add columns to the class
        attrs = {'__tablename__': table_name, '__table__': table}

        # Inspect each column from the table and add it to the attributes
        for column in table.columns:
            if column.name not in attrs:  # Avoid overwriting any default columns like 'geom'
                attrs[column.name] = column

        # Create a new class with dynamically loaded columns
        return type(table_name, (Base,), attrs)


    @classmethod
    def to_geodataframe(cls, rows):
        import geopandas as gpd
        from shapely import wkb
        """Converts SQLAlchemy row results to a GeoDataFrame."""
        data = []
        for row in rows:
            data.append({
                'id': row.id,
                'name': row.name,
                'geometry': wkb.loads(bytes(row.geom.data))  # Convert WKB to shapely geometry
            })
        return gpd.GeoDataFrame(data, geometry='geometry', crs='EPSG:4326')


# Database connection and querying class
class SpatialDatabaseManager:
    def __init__(self, db_name):
        self.engine = get_postgis_engine(database=db_name, user='t968rs')
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)  # Ensures tables are created if not existing

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def query_intersecting_geometries(self, geometry_wkt):
        """Queries rows that intersect with the provided geometry (WKT format)."""
        with self.session_scope() as session:
            rows = session.query(PostGIS_Table).filter(
                ST_Intersects(PostGIS_Table.geom, func.ST_GeomFromText(geometry_wkt, 4326))
            ).all()
        return rows



# Example usage
if __name__ == "__main__":
    # Replace with actual database connection info
    database_url = get_sql_url('localhost', 't968rs')

    # Instantiate the spatial database manager
    manager = SpatialDatabaseManager(database_url)

    # Define a bounding box (replace with actual coordinates)
    bbox_wkt = bbox_to_wkt(-104.9903, 39.7392, -104.9803, 39.7492)

    # Query intersecting geometries
    intersecting_rows = manager.query_intersecting_geometries(bbox_wkt)

