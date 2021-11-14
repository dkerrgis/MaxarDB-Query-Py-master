import pytest
from pathlib import Path 
import geopandas as gpd
import numpy as np

from pandas._testing import assert_frame_equal
import sqlalchemy
from sqlalchemy import create_engine
from psycopg2 import OperationalError

from settings import DB_CREDENTIALS

from building_query import QueryDB, QueryDBMulti, PolygonAreaException, GeoDataFrameException

BASE_DIR = Path(__file__).resolve().parent
INPUT_SHP_9 = BASE_DIR.joinpath('test_data/test_grid_9.shp')
INPUT_SHP_1 = BASE_DIR.joinpath('test_data/test_grid_1.shp')

OUTPUT_SHP_9 = BASE_DIR.joinpath('test_data/out_9.shp')
OUTPUT_SHP_1 = BASE_DIR.joinpath('test_data/out_1.shp')

@pytest.fixture
def MTQueryAdmin():
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	assert user == 'wpuser'
	query_table = f'{schema}.{table}'
	x = QueryDBMulti(INPUT_SHP_9, query_table, user, password, num_threads=9)
	return x

def test_object_instantiation(MTQueryAdmin):
    assert isinstance(MTQueryAdmin, QueryDBMulti)

def test_multithread_functionality(MTQueryAdmin):
    x = MTQueryAdmin
    gdf_all_buildings = x.execute_query_multithread()
    assert isinstance(gdf_all_buildings, gpd.GeoDataFrame)

def test_output_from_multi_compared_to_single(MTQueryAdmin):
    x = MTQueryAdmin
    gdf_all_buildings = x.execute_query_multithread()
    out_path = OUTPUT_SHP_1.parent.joinpath('OUT_MT.shp')
    x.save_shp(gdf_all_buildings, out_path)
    gdf_mt = gpd.read_file(str(out_path)).set_index('gid').sort_index()
    gdf_st = gpd.read_file(str(OUTPUT_SHP_1)).set_index('gid').sort_index()
    assert gdf_mt.shape == gdf_st.shape
    assert np.array_equal(gdf_mt.values, gdf_st.values)
