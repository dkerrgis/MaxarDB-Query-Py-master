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

########################################################################
####################### FIXTURES #######################################
########################################################################
@pytest.fixture
def STQueryAdmin():
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	assert user == 'wpuser'
	query_table = f'{schema}.{table}'
	x = QueryDB(INPUT_SHP_9, query_table, user, password)
	return x

@pytest.fixture
def STQueryUser():
	DB_CRED = DB_CREDENTIALS['USER']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	assert user == 'wpselect'
	query_table = f'{schema}.{table}'
	x = QueryDB(INPUT_SHP_9, query_table, user, password)
	return x 

@pytest.fixture
def STQueryAdminGDF():
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	assert user == 'wpuser'
	query_table = f'{schema}.{table}'
	INPUT_GDF = gpd.read_file(str(INPUT_SHP_1))
	x = QueryDB(INPUT_GDF, query_table, user, password)
	return x

########################################################################
########################################################################
########################################################################


########################################################################
#################################SETUP TESTS############################
########################################################################

def test_settings_imported():
	assert DB_CREDENTIALS['ADMIN']['user'] == 'wpuser'
	assert DB_CREDENTIALS['USER']['user'] == 'wpselect'


def test_object_instantiation_admin(STQueryAdmin):
	x = STQueryAdmin
	assert isinstance(x, QueryDB)
	assert x.user == 'wpuser'

def test_object_instantiation_user(STQueryUser):
	x = STQueryUser
	assert isinstance(x, QueryDB)
	assert x.user == 'wpselect'

def test_exception_raised_with_wrong_input_shp_path_type():
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	INPUT_SHP_WRONG = 6
	with pytest.raises(Exception) as e:
		x = QueryDB(INPUT_SHP_WRONG, table, users, password)

def test_input_file_exception_no_found():
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	INPUT_SHP_WRONG_DOESNTEXIST = 'not_here.shp'
	with pytest.raises(Exception) as e:
		x = QueryDB(INPUT_SHP_WRONG_DOESNTEXIST, table, users, password)

def test_input_as_gdf_okay(STQueryAdminGDF):
	x = STQueryAdminGDF
	expected_gdf = gpd.read_file(str(INPUT_SHP_1))
	assert isinstance(x.input_shp, gpd.GeoDataFrame)
	assert_frame_equal(x.input_shp, expected_gdf)

def test_correct_schema_table_set_up(STQueryAdmin):
	assert STQueryAdmin.query_table == 'maxar_building_v1.ben'

########################################################################
########################################################################
########################################################################

########################################################################
############################## FUNTIONALITY ############################
########################################################################

def test_get_connection(STQueryAdmin):
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	con = STQueryAdmin.get_connection()
	con_expected = create_engine(f'postgresql://{user}:{password}@db.worldpop.org:5432/maxar')
	assert con_expected.url == con.url

def test_get_connection_with_wrong_credentials_usernam(STQueryAdmin):
	STQueryAdmin.user = 'Wrong_name'
	with pytest.raises(sqlalchemy.exc.OperationalError) as e:
		con = STQueryAdmin.get_connection()

def test_get_connection_with_wrong_credentials_password(STQueryAdmin):
	STQueryAdmin.password = 'not_password'
	with pytest.raises(sqlalchemy.exc.OperationalError) as e:
		con = STQueryAdmin.get_connection()

def test_get_geoms_from_input_shp(STQueryAdmin):
	geoms = STQueryAdmin.get_geoms()
	gdf_expected = gpd.read_file(str(INPUT_SHP_9))
	geoms_expected = [x.geometry for index, x in gdf_expected.iterrows()]
	assert geoms == geoms_expected

def test_get_geoms_from_input_gdf(STQueryAdminGDF):
	gdf_expected = gpd.read_file(str(INPUT_SHP_1))
	geoms_expected = [x.geometry for index, x in gdf_expected.iterrows()]
	geoms = STQueryAdminGDF.get_geoms()
	assert geoms == geoms_expected

def test_extract_from_db(STQueryAdmin):
	x = STQueryAdmin
	con = x.get_connection()
	geoms = x.get_geoms()
	gdf = x.extract(geoms[0], con)
	assert isinstance(gdf, gpd.GeoDataFrame)

@pytest.mark.skip(reason="Long-running")
def test_looping_through_geoms(STQueryAdmin):
	x = STQueryAdmin
	gdf_all_buildings = x.execute_query()
	assert isinstance(gdf_all_buildings, gpd.GeoDataFrame)
	
def test_looping_with_one_polygon_in_shp(STQueryAdminGDF):
	x = STQueryAdminGDF
	gdf_all_buildings = x.execute_query()
	assert isinstance(gdf_all_buildings, gpd.GeoDataFrame)

def test_save_shp(STQueryAdminGDF):
	x = STQueryAdminGDF
	gdf_all_buildings = x.execute_query()
	gdf_all_buildings.to_file(str(OUTPUT_SHP_1.parent.joinpath('TEST.shp')))
	gdf_all_buildings = gpd.read_file(str(OUTPUT_SHP_1.parent.joinpath('TEST.shp')))
	print(f'First gdf is {len(gdf_all_buildings)}')
	x.save_shp(gdf_all_buildings, OUTPUT_SHP_1)
	gdf = gpd.read_file(str(OUTPUT_SHP_1))
	print(f'Second gdf is {len(gdf)}')
	assert np.array_equal(gdf.values, gdf_all_buildings.values)

def test_full_system_with_user_credentials(STQueryUser):
	x = STQueryUser
	gdf_all_buildings = x.execute_query()
	gdf_all_buildings.to_file(str(OUTPUT_SHP_1.parent.joinpath('TEST.shp')))
	gdf_all_buildings = gpd.read_file(str(OUTPUT_SHP_1.parent.joinpath('TEST.shp')))
	x.save_shp(gdf_all_buildings, OUTPUT_SHP_1)
	gdf = gpd.read_file(str(OUTPUT_SHP_1))
	assert np.array_equal(gdf.values, gdf_all_buildings.values)


@pytest.mark.large_area_exception
def test_exception_raised_for_too_large_area():
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	assert user == 'wpuser'
	query_table = f'{schema}.{table}'
	x = QueryDB(INPUT_SHP_9.parent.joinpath('test_grid_10km_9.shp'), query_table, user, password)
	with pytest.raises(PolygonAreaException):
		gdf_all_buildings = x.execute_query()

@pytest.mark.wrong_prj
def test_exception_when_prj_not_4326():
	DB_CRED = DB_CREDENTIALS['ADMIN']
	user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
	assert user == 'wpuser'
	query_table = f'{schema}.{table}'
	with pytest.raises(GeoDataFrameException):
		x = QueryDB(INPUT_SHP_9.parent.joinpath('be_grid_project.shp'), query_table, user, password)

########################################################################
########################################################################
########################################################################






