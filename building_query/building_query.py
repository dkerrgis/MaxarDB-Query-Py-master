"""Module to define classes to query WP Postgis db holding Maxar builing footprints for SS Africa

class QueryDB --> Query table using single thread for each polygon in each input shape
class QueryDBMulti --> Query table using multi-threads defined by user input
"""
from pathlib import Path 
import geopandas as gpd 
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from psycopg2 import OperationalError
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
from functools import partial



class QueryDB:
	"""Query table using single thread for each polygon in each input shape"""
	def __init__(self, input_shp, query_table, user, password, chunksize=200):
		"""
		Instantiation

		Parameters:
		input_shp (Path/str) :: Path to input shapefile to query
		query_table (str) :: table to query <SCHEMA.table_name> format
		user (str) :: User name
		password (str) :: Password
		chunksize (int) :: Chunksize of table to return in each iteration
		"""
		self.input_shp = input_shp
		if not isinstance(self.input_shp, Path):
			if not isinstance(self.input_shp, gpd.GeoDataFrame):
				try:
					self.input_shp = Path(self.input_shp).resolve()
				except Exception as e:
					raise e
		if not isinstance(self.input_shp, gpd.GeoDataFrame):
			if self.input_shp.exists():
				try:
					self.gdf = gpd.read_file(str(self.input_shp))
				except Exception as e:
					raise e
			else:
				raise FileNotFoundError('Input shapefile does not exist')
		else:
			self.gdf = self.input_shp #<---input shape was geopandas dataframe
		if not self.gdf.crs.name == 'WGS 84':
			raise GeoDataFrameException("Projection Error. Shapefile should be in WGS84")
		self.query_table = query_table
		self.user = user
		self.password = password
		self.chunksize = chunksize

	def get_connection(self):
		"""Returns SQLAlchemy engine used to connect to database
		
		Parameters:
		None

		Returns:
		con (sqlalchemy.engine.base.Engine) :: Connection to the database
		"""
		url = f"postgresql://{self.user}:{self.password}@db.worldpop.org:5432/maxar"
		con = create_engine(url)
		try:
			con.connect()
		except sqlalchemy.exc.OperationalError as e:
			raise e
		else:
			return con

	def get_geoms(self):
		"""
		Returns list of geometries for each polygon in self.input_shp

		Parameters:
		None

		Returns:
		geoms (list) :: List of shapely polygon geometries
		"""
		geoms = [x.geometry for index, x in self.gdf.iterrows()]
		largest_geom = self.find_largest_area(geoms)
		if largest_geom > 100000000:
			raise PolygonAreaException(f"Largest input polygon is {largest_geom * 0.000001} km**2. Try to reduce max area of single polygon to 100km**2")
		else:
			return geoms

	def find_largest_area(self, geoms):
		"""Returns area of largest geometry in list of geometries
		
		Parameters:
		geoms (list) :: list of shapely polygons

		Returns:
		max_prj_area (float) :: Largest area in meters^2
		"""
		max_prj_area = max([self.projected_area(x) for x in geoms])
		#max_prj_area = self.projected_area(geoms)
		return max_prj_area
		


	def projected_area(self, geom):
		"""Returns area projected from WGS84 to 3857
		
		Parameters:
		geom (shapely.geometry.Polygon) :: Polygon

		Returns:
		area (float) :: Area
		"""
		s = shape(geom)
		proj = partial(pyproj.transform, pyproj.Proj('epsg:4326'),
					pyproj.Proj('epsg:3857'))

		s_new = transform(proj, s)

		area = transform(proj, s).area
		return area


	def execute_query(self):
		"""Returns gdf of data returned from query defined in instantiation
		
		Parameters:
		geoms (list) :: List of shapely geoms

		Returns:
		gdf (gpd.GeoDataFrame) :: Geodataframe of building footprints
		"""
		con = self.get_connection()
		geoms = self.get_geoms()
		gdf_all = []
		for geom in geoms:
			gdf_sub = self.extract(geom, con)
			gdf_all.append(gdf_sub)
		gdf_all_buildings = gpd.GeoDataFrame(pd.concat(gdf_all))
		return gdf_all_buildings

	def extract(self, geom, con):
		"""Returns gdf of buildings intersecting geom
		
		Parameters:
		geom (shapely.geometry.Polygon) :: Geometry of polygon
		con (sqlalchemy.engine.base.Engine) :: Connection to DB

		Returns:
		gdf (gpd.GeoDataFrame) :: Geodataframe of buildings
		"""
		sql = f"select b.gid, b.wp_iso, b.area, b.perimeter, b.geom as geometry from {self.query_table} b where st_intersects(st_setsrid(st_geomfromtext('{geom.wkt}'), 4326), b.centroid_geom);"
		try:
			gdf = gpd.GeoDataFrame(pd.concat([chunk for chunk in gpd.read_postgis(sql, con, geom_col='geometry', chunksize=self.chunksize)]))
		except Exception as e:
			raise e
		else:
			return gdf

	def save_shp(self, gdf, out_path):
		"""Saves input gdf to out_path as shp
		
		Parameters:
		gdf (gpd.GeoDataFrame) :: gdf of buildings
		out_path (str/Path) :: path to output shapefile

		Returns:
		None
		"""
		try:
			gdf.to_file(out_path)
		except Exception as e:
			raise e



class QueryDBMulti(QueryDB):
	"""Query table using multi-threads defined by user input"""
	def __init__(self, input_shp, query_table, user, password, chunksize=200, num_threads=10):
		"""
		Instantiation

		Parameters:
		input_shp (Path/str) :: Path to input shapefile to query
		query_table (str) :: table to query <SCHEMA.table_name> format
		user (str) :: User name
		password (str) :: Password
		chunksize (int) :: Chunksize of table to return in each iteration
		num_threads (int) :: Number of threads to use to query DB (Default = 10)
		"""
		super().__init__( input_shp, query_table, user, password, chunksize)
		self.num_threads = int(num_threads)

	def execute_query_multithread(self):
		"""Executes DB query in multithreads
		
		Parameters:
		None

		Returns:
		gdf_all_buildings (gpd.GeoDataFrame) :: GeoDataFrame of all building footprints

		"""
		con = self.get_connection()
		geoms = self.get_geoms()
		gdf_buildings_completed = []
		with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
			future_to_buildings = {executor.submit(self.extract, geom, con): geom for geom in geoms}
			for future in as_completed(future_to_buildings):
				res = future_to_buildings[future]
				try:
					data = future.result()
				except:
					print(f'EXCEPTION - {res}')
				else:
					gdf_buildings_completed.append(data)
		gdf_buildings_completed = gpd.GeoDataFrame(pd.concat(gdf_buildings_completed))
		return gdf_buildings_completed

class GeoDataFrameException(Exception):
	pass

class PolygonAreaException(Exception):
	pass