from pathlib import Path 
import geopandas as gpd
import numpy as np

from building_query import QueryDB, QueryDBMulti

from settings import DB_CREDENTIALS ###Dictionary with Passwords etc as below:
"""DB_CREDENTIALS = {
	'USER': {
		'user': 'wpselect',
		'password': '***********',
		'host': 'db.worldpop.org',
		'schema': 'maxar_building_v1',
		'table': 'ben', <------------This needs to change for each country
		'port': 5432,
	} 
}"""

# NB ['ben', 'nga', 'ner', 'tcd', 'cmr', 'cod', 'ago'] are the only available tables in the schema maxar_building_v1 at the moment.

BASE_DIR = Path(__file__).resolve().parent 
DATA_DIR = BASE_DIR.joinpath('tests/test_data')

INPUT_SHP = DATA_DIR.joinpath('test_grid_9.shp')
OUTPUT_SHP = DATA_DIR.joinpath('TEST_OUTPUT.shp')

def main():    
    DB_CRED = DB_CREDENTIALS['USER']
    user, password, host, schema, table, port = (DB_CRED[x] for x in DB_CRED.keys())
    query_table = f'{schema}.{table}' #<-- Should be <schema>.<table>
    query = QueryDBMulti(INPUT_SHP, query_table, user, password, num_threads=15) #<--num_threads default is 10
    gdf_all_buildings = query.execute_query_multithread() #<--get geodataframe of footprints
    query.save_shp(gdf_all_buildings, OUTPUT_SHP) #<-- Save footprints


if __name__ == "__main__":
    main()