import math
import os
import matplotlib
import numpy as np
import h3
from sklearn.cluster import DBSCAN
from dask import delayed, compute
import pandas as pd
from geopy import Nominatim
from pandas import DataFrame
import geopandas as gpd
import geojson
import contextily as ctx
import matplotlib.pyplot as plt
import dask.dataframe as dd
from scipy.spatial.distance import pdist, squareform
from scipy.optimize import linear_sum_assignment
from concurrent.futures import ThreadPoolExecutor

class Utils:
    global geolocator
    geolocator = Nominatim(user_agent="my_geocoder")

    def __init__(self):
        matplotlib.use('Agg')

    def haversine(self, lat1, lon1, lat2, lon2):
        """
        Calculate distance of one point to another using haversine
        :param float lat1: First position latitude
        :param float lon1: Second position longitude
        :param float lat2: First position latitude
        :param float lon2: Second position longitude
        :return: Haversine distance in Kilometer.
        """
        # Earth radius in km
        R = 6371.0

        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)

        dif_latitude = lat2 - lat1
        dif_longitude = lon2 - lon1

        # Haversine formula
        a = math.sin(dif_latitude / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dif_longitude / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c
        return distance

    def get_column_types(self, dataframe):
        """
        Get columns types values
        :param DataFrame dataframe: panda dataframe
        :return: columns type of the dataframe.
        """
        column_types = []
        try:
            for column, dtype in dataframe.dtypes.items():
                # Create a dictionary for each column
                column_types.append({
                    column: str(dtype)  # Convert the data type to string for output
                })
        except:
            print("An exception occurred")
        return column_types

    def create_data_frame_from_csv(self, file_path, separator):
        """
        Get columns types values
        :param string file_path: csv file path
        :param string separator: csv separator
        :return: dataframe values
        """
        return pd.read_csv(file_path, sep=separator)

    def create_data_frame_array(self, base_path, initial_year, separator, file_len):
        """
        Get columns types values
        :param string base_path: csv file base path
        :param int initial_year: initial year to search
        :param string separator: csv separator
        :param int file_len: how many files to search
        :return: list of tuple with year value and dataframe
        """
        data_frame_array = []
        for year in range(file_len):
            year_str = str(initial_year + year)
            df = self.create_data_frame_from_csv(f'{base_path}{year_str}.csv', separator)
            df['ano'] = year_str
            data_frame_array.append((year_str, df))
        return data_frame_array

    def append_all_dataframes(self, data_frame_array):
        """
        Append all DataFrames from a list of (year, DataFrame) tuples into a single DataFrame
        :param data_frame_array: list of year and DataFrame tuples
        :return: a single concatenated DataFrame
        """
        # Extract the DataFrames from the list of tuples
        frames = [df for year_str, df in data_frame_array]
        # Concatenate all DataFrames along rows (axis=0)
        concatenated_df = pd.concat(frames, axis=0, ignore_index=True)
        return concatenated_df

    def filter_data_frame_array_by_year(self, data_frame_array, target_year):
        """
        Get count of values repeated in the column
        :param DataFrame data_frame_array: dataframe array
        :param string target_year: year to filter
        :return: filtered array of dataframe
        """
        # Ensure the year is a string for comparison
        target_year_str = str(target_year)
        # Filter the tuples based on the year (first element of the tuple)
        filtered_array = [tup for tup in data_frame_array if tup[0] == target_year_str]
        return filtered_array

    def get_column_values_repeated(self, dataframe_data, column_name):
        """
        Get count of values repeated in the column
        :param DataFrame dataframe_data: csv file base path
        :param string column_name: initial year to search
        :return: count of values repeated in the column
        """
        return dataframe_data[f'{column_name}'].duplicated().sum()

    def is_dataframe_null_values(self, dataframe_data) :
        """
        Check if dataframe have null values
        :param DataFrame dataframe_data: csv file base path
        :return: true if dataframe hava null values, false otherwise
        """
        return dataframe_data[dataframe_data.isnull().T.any()]

    def filter_dataframe_column(self, dataframe, column_name, filter_value):
        """
        get postcode name based in latitude and longitude using geolocator
        :param DataFrame dataframe: panda dataframe
        :param string column_name: column name to filter
        :param string filter_value: value to filter
        :return: string with neighborhood value.
        """
        return dataframe[dataframe[f'{column_name}'] == filter_value]

    def get_postcode_from_lat_and_lon(self, latitude, longitude):
        """
        get postcode name based in latitude and longitude using geolocator
        :param string latitude: map position value
        :param string longitude: map position value
        :return: string with neighborhood value.
        """
        location = geolocator.reverse((latitude, longitude), exactly_one=True, language='pt-BR')
        if location:
            address = location.raw.get('address', {})
            neighborhood = address.get('postcode', None)
            if neighborhood:
                return neighborhood
            else:
                return 'NONE'
        else:
            return 'NONE'

    def get_neighborhood(self, latitude, longitude):
        """
        get neighborhood name based in latitude and longitude using geolocator
        :param string latitude: map position value
        :param string longitude: map position value
        :return: string with neighborhood value.
        """
        location = geolocator.reverse((latitude, longitude), exactly_one=True, language='pt-BR')
        if location:
            address = location.raw.get('address', {})
            neighborhood = address.get('neighbourhood', None)
            if neighborhood:
                return neighborhood
            else:
                return 'NONE'
        else:
            return 'NONE'

    def add_CEP_in_df(self, dataframe):
        """
        add BAIRRO in a dataframe
        :param DataFrame dataframe: panda dataframe
        :return: dataframe.
        """
        dataframe['CEP'] = 'NONE'
        for index, row_data in dataframe.iterrows():
            lat = row_data['lat']
            lon = row_data['lon']
            postcode = self.get_postcode_from_lat_and_lon(lat, lon)
            dataframe.at[index, 'CEP'] = postcode if postcode else 'NONE'
        return dataframe

    def add_BAIRRO_in_df(self, dataframe):
        """
        add BAIRRO in a dataframe
        :param DataFrame dataframe: panda dataframe
        :return: dataframe.
        """
        dataframe['BAIRRO'] = 'NONE'
        for index, row_data in dataframe.iterrows():
            lat = row_data['lat']
            lon = row_data['lon']
            postcode = self.get_neighborhood(lat, lon)
            dataframe.at[index, 'BAIRRO'] = postcode if postcode else 'NONE'
        return dataframe

    def set_dataframe_for_city(self, dataframe):
        """
        add BAIRRO and CEP columns in a dataframe
        :param DataFrame dataframe: panda dataframe
        :return: dataframe.
        """
        self.add_CEP_in_df(dataframe)
        self.add_BAIRRO_in_df(dataframe)
        return dataframe

    def add_h3_index(self, dataframe, resolution):
        """
        apply H3 spatial indexing at a resolution level appropriate for max_distance_km
        :param DataFrame dataframe: panda dataframe
        :param int resolution: resolution value equivalent to Km² of hexagon area
        :return: dataframe.
        """
        dataframe['h3_index'] = dataframe.apply(lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], resolution), axis=1)
        dataframe['hex_area_km2'] = dataframe['h3_index'].apply(lambda h3_index: h3.cell_area(h3_index, unit='km^2'))
        return dataframe

    def cluster_h3_cell(self, dataframe, max_distance_km):
        """
        apply DBSCAN on each H3 hex cell cluster
        :param DataFrame dataframe: panda dataframe
        :param float max_distance_km: max distance in kilometer between burned area
        :return: dataframe.
        """
        coordinates = dataframe[['latitude', 'longitude']].to_numpy()
        kms_per_radian = 6371.0088
        km_to_radians = max_distance_km / kms_per_radian
        db = DBSCAN(eps=km_to_radians, min_samples=2, algorithm='ball_tree', metric='haversine').fit(np.radians(coordinates))
        dataframe['cluster'] = db.labels_
        return dataframe

    def run_dbscan(self, dataframe, distance_in_kilometers):
        """
        find the nearest burned area points in a valid cluster
        :param DataFrame dataframe: panda dataframe
        :param float distance_in_kilometers: max distance in kilometer between burned area 
        :return: dataframe.
        """
        clustered_dfs = []
        for h3_index, points in dataframe.groupby('h3_index'):
            delayed_cluster = delayed(self.cluster_h3_cell)(points, distance_in_kilometers)  # No need to pass h3_index
            clustered_dfs.append(delayed_cluster)

        clustered_results = compute(*clustered_dfs)
        return pd.concat(clustered_results)

    def find_nearest_points_in_valid_cluster(self, dataframe):
        """
        find the nearest burned area points in a valid cluster
        :param DataFrame dataframe: panda dataframe
        :return: geo json data.
        """
        df_valid_clusters = dataframe[dataframe['cluster'] >= 0]
        def process_chunk(chunk):
            features = []
            for _, row in chunk.iterrows():
                point = geojson.Point((row['longitude'], row['latitude']))
                properties = row.drop(['latitude', 'longitude']).to_dict()
                properties['latitude'] = row['latitude']
                properties['longitude'] = row['longitude']

                feature = geojson.Feature(geometry=point, properties=properties)
                features.append(feature)
            return features

        ddf = dd.from_pandas(df_valid_clusters, npartitions=4)
        delayed_results = ddf.map_partitions(lambda df: delayed(process_chunk)(df))
        features_lists = compute(*delayed_results)
        all_features = [feature for sublist in features_lists for feature in sublist]
        return geojson.FeatureCollection(all_features)

    def export_geojson_for_valid_cluster(self, dataframe, max_distance_km, resolution):
        """
        Create cluster regions based in H3 resolution and max distance between burned areas
        :param DataFrame dataframe: panda dataframe
        :param float max_distance_km: max distance between burned areas
        :param int resolution: resolution value equivalent to Km² of hexagon area
        :return: dataframe data.
        """
        df_sorocaba = self.add_h3_index(dataframe, resolution=resolution)
        clustered_df = self.run_dbscan(df_sorocaba, max_distance_km)
        return clustered_df

    def set_geo_json_file(self, clustered_df, file_name):
        """
        Create Geo json file with burned area map data
        :param DataFrame clustered_df: panda dataframe
        :param string file_name: file name
        """
        geojson_data = self.find_nearest_points_in_valid_cluster(clustered_df)
        if not os.path.exists(file_name + '.geojson'):
            with open(file_name + '.geojson', 'w') as f:
                geojson.dump(geojson_data, f)

    def export_excel_with_sheets(self, dataframes, file_name):
        """
        Create Excel file using the
        :param DataFrame dataframes: panda dataframe
        :param string file_name: file name
        """
        file_path = file_name + '.xlsx'
        if not os.path.exists(file_path):
            with pd.ExcelWriter(file_path, mode='w') as writer:
                for sheet_name, df in dataframes.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

    def save_cluster_points_images(self, city_name):
        """
        Create map image with the burned areas pointed
        :param string city_name: city name
        """
        gdf = gpd.read_file(city_name + '.geojson')
        # Configure the coordinate system for gdf and add the basemap
        # CRS for coordinates compatible with map context
        gdf = gdf.to_crs(epsg=3857)
        fig, ax = plt.subplots(figsize=(10, 10))
        gdf.plot(ax=ax, color='red', marker='x', markersize=80, alpha=0.5, label="Focos de queimada")

        minx, miny, maxx, maxy = gdf.total_bounds
        margin_x = (maxx - minx) * 0.05
        margin_y = (maxy - miny) * 0.05
        ax.set_xlim(minx - margin_x, maxx + margin_x)
        ax.set_ylim(miny - margin_y, maxy + margin_y)

        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, crs='EPSG:3857')
        ax.set_title("Focos próximos de queimada GeoJSON - " + city_name, fontsize=15)
        plt.legend()
        plt.savefig(city_name + ".png", dpi=300)
        plt.close(fig)

    def order_by_proximity(self, dataframe):
        """
        Arranges the DataFrame rows to minimize the total Euclidean distance traveled between consecutive points in the list
        :param DataFrame dataframe: panda dataframe
        :return: order the dataFrame by min distances.
        """
        coords = dataframe[['latitude', 'longitude']].values
        dist_matrix = squareform(pdist(coords, metric='euclidean'))
        row_ind, col_ind = linear_sum_assignment(dist_matrix)
        ordered_df = dataframe.iloc[col_ind].reset_index(drop=True)
        return ordered_df

    def calculate_distance(self, i, dataframe):
        """
        Calculate one distance of one row to the next row
        :param DataFrame dataframe: panda dataframe
        :param int i: row position
        :return: Distance in Kilometer.
        """
        lat1, lon1 = dataframe.iloc[i][['latitude', 'longitude']]
        lat2, lon2 = dataframe.iloc[i + 1][['latitude', 'longitude']]
        return self.haversine(lat1, lon1, lat2, lon2)

    def calculate_distances_parallel(self, dataframe):
        """
        Calculate distance of one row to the next row
        :param DataFrame dataframe: panda dataframe
        :return: Distances in Kilometer.
        """
        distances = [None] * (len(dataframe) - 1)
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(self.calculate_distance, i, dataframe) for i in range(len(dataframe) - 1)]
            for i, future in enumerate(futures):
                distances[i] = future.result()

        # Last row data
        distances.append(None)

        return distances

    def set_dataframe_data(self, dataframe, city_name, max_distance_km, resolution):
        """
        Set dataframe data
        :param DataFrame dataframe: panda dataframe
        :param string city_name: city name
        :param float max_distance_km: max distance in kilometer between burned area
        :param int resolution: resolution value equivalent to Km² of hexagon area
        :return: dataframe data.
        """
        if not os.path.exists(city_name + '.geojson') and not os.path.exists(city_name + ".png"):
            df_city = dataframe[dataframe['municipio'] == city_name]
            df_city = df_city.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
            df_city = self.export_geojson_for_valid_cluster(df_city, max_distance_km, resolution)
            self.set_geo_json_file(df_city, city_name)
            self.save_cluster_points_images(city_name)

            # remove the invalid cluster
            df_city = df_city[df_city['cluster'] != -1]
            df_city = df_city.sort_values('cluster').reset_index(drop=True)
            df_city = (
                df_city.groupby('cluster', group_keys=False)
                .apply(self.order_by_proximity)
                .reset_index(drop=True)
            )
            df_city['distance_to_next'] = self.calculate_distances_parallel(df_city)
            return df_city

