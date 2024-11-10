import json
import math
import os
from typing import Hashable

import matplotlib
import numpy as np
import h3
from shapely import Polygon, Point
from sklearn.cluster import DBSCAN
from dask import delayed, compute

import pandas as pd
from geopy import Nominatim
from pandas import DataFrame
from geopy.distance import geodesic
import geopandas as gpd
import geojson
import contextily as ctx
import matplotlib.pyplot as plt
import dask.dataframe as dd
matplotlib.use('Agg')

class Utils:
    global geolocator
    geolocator = Nominatim(user_agent="my_geocoder")
    def __init__(self, name):
        self.name = name

    # Função para calcular distâncias e retornar objetos com os IDs
    def calcular_distancias_abaixo_threshold(self, lat_lon_array, threshold_km):
        resultados_below_threshold = []

        # Iterar sobre todos os pares (i, j)
        for i in range(len(lat_lon_array)):
            for j in range(i + 1, len(lat_lon_array)):
                lat1, lon1, foco_id_1 = lat_lon_array[i]
                lat2, lon2, foco_id_2 = lat_lon_array[j]

                # Calcular a distância Haversine
                distancia = self.haversine(lat1, lon1, lat2, lon2)

                # Verificar se a distância é menor que o threshold
                if distancia < threshold_km:
                    # Armazenar o par de IDs em um dicionário
                    resultados_below_threshold.append({
                        'foco_id_1': foco_id_1,
                        'foco_id_2': foco_id_2
                    })

        return resultados_below_threshold

    # Função que implementa a fórmula de Haversine
    def haversine(self, lat1, lon1, lat2, lon2):
        # Raio da Terra em km
        R = 6371.0

        # Converter de graus para radianos
        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)

        # Diferenças de coordenadas
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        # Fórmula de Haversine
        a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        # Distância em km
        distance = R * c
        return distance

    def get_column_types(self, dataframe) -> list[dict[Hashable, str]]:
        """
        Get columns types values
        :param DataFrame dataframe: panda dataframe
        :return: columns type of the dataframe.
        :rtype: list[dict[Hashable, str]]
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


    def create_data_frame_from_csv(self, filePath, separator) -> DataFrame:
        """
        Get columns types values
        :param string filePath: csv file path
        :param string separator: csv separator
        :return: dataframe values
        :rtype: DataFrame
        """
        return pd.read_csv(filePath, sep=separator)

    def create_data_frame_array(self, basePath, initial_year, separator, file_len) -> list[tuple[str, DataFrame]]:
        """
        Get columns types values
        :param string basePath: csv file base path
        :param int initial_year: initial year to search
        :param string separator: csv separator
        :param int file_len: how many files to search
        :return: list of tuple with year value and dataframe
        :rtype: list[tuple[str, DataFrame]]
        """
        data_frame_array = []

        for year in range(file_len):
            year_str = str(initial_year + year)
            df = self.create_data_frame_from_csv(f'{basePath}{year_str}.csv', separator)
            df['ano'] = year_str
            data_frame_array.append((year_str, df))

        return data_frame_array

    def append_all_dataframes(self, data_frame_array: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
        """
        Append all DataFrames from a list of (year, DataFrame) tuples into a single DataFrame
        :param list[tuple[str, DataFrame]] data_frame_array: list of year and DataFrame tuples
        :return: a single concatenated DataFrame
        :rtype: pd.DataFrame
        """
        # Extract the DataFrames from the list of tuples
        frames = [df for year_str, df in data_frame_array]

        # Concatenate all DataFrames along rows (axis=0)
        concatenated_df = pd.concat(frames, axis=0, ignore_index=True)

        return concatenated_df

    def filter_data_frame_array_by_year(self, data_frame_array, target_year):
        # Ensure the year is a string for comparison
        target_year_str = str(target_year)

        # Filter the tuples based on the year (first element of the tuple)
        filtered_array = [tup for tup in data_frame_array if tup[0] == target_year_str]

        return filtered_array



    def get_column_values_repeated(self, dataframe_data, column_name) -> int:
        """
        Get count of values repeated in the column
        :param DataFrame dataframe_data: csv file base path
        :param string column_name: initial year to search
        :return: count of values repeated in the column
        :rtype: int
        """
        return dataframe_data[f'{column_name}'].duplicated().sum()

    def is_dataframe_null_values(self, dataframe_data) -> bool:
        """
        Check if dataframe have null values
        :param DataFrame dataframe_data: csv file base path
        :return: true if dataframe hava null values, false otherwise
        :rtype: bool
        """
        return dataframe_data[dataframe_data.isnull().T.any()]

    def get_dataframe_by_year(self, data_frame_array, target_year):
        target_year_str = str(target_year)

        filtered_tuple = next((tup for tup in data_frame_array if tup[0] == target_year_str), None)

        return filtered_tuple[1] if filtered_tuple else None

    def count_of_data(self, data, municipio):
        data = self.filter_dataframe_column(data, 'municipio', municipio)

        if data['municipio'].count() > 0:
            print(f'Focos de queimadas em {municipio} desde 2003: {data["municipio"].count()}')

    def test(self, all_dataframes):
        # List of years corresponding to the DataFrames
        years = list(range(2003, 2024))

        # List to store the result DataFrames of each year
        yearly_results = []

        # Dictionary to collect the count of all cities across all years for the summary sheet
        summary_dict = {}

        for i, df in enumerate(all_dataframes):
            year = years[i]

            # Get value counts for 'municipio' column
            value_counts = df.municipio.value_counts()

            # Create a DataFrame for the year
            result_df = pd.DataFrame({
                'Cidades': value_counts.index,  # First column: Unique city names
                'Focos': value_counts.values  # Second column: Count of fire occurrences
            })

            # Add the result to the yearly_results list for individual year tabs
            yearly_results.append((year, result_df))

            # Update the summary_dict to sum up counts across all years
            for city, focos in zip(value_counts.index, value_counts.values):
                if city in summary_dict:
                    summary_dict[city] += focos
                else:
                    summary_dict[city] = focos

        # Convert the summary_dict to a DataFrame
        summary_df = pd.DataFrame({
            'Cidades': summary_dict.keys(),
            'Total Focos': summary_dict.values()
        })

        # Sort the summary DataFrame by 'Total Focos' in descending order
        summary_df = summary_df.sort_values(by='Total Focos', ascending=False)

        # Create an Excel writer
        with pd.ExcelWriter('focos_queimadas_summary_sorted.xlsx') as writer:
            # Write the sorted combined summary DataFrame to the first sheet
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

            # Write each year result to its own sheet
            for year, result_df in yearly_results:
                result_df.to_excel(writer, sheet_name=str(year), index=False)

    def filter_dataframe_column(self, dataframe_data, column_name, filter_value):
        return dataframe_data[dataframe_data[f'{column_name}'] == filter_value]

    def get_postcode_from_lat_and_lon(self, lat, lon):
        # Reverse geocode the latitude and longitude
        location = geolocator.reverse((lat, lon), exactly_one=True, language='pt-BR')

        if location:
            # Access the raw data
            address = location.raw.get('address', {})

            # Get the neighborhood if available
            neighborhood = address.get('postcode', None)

            if neighborhood:
                return neighborhood
            else:
                return 'NONE'
        else:
            return 'NONE'

    def get_neighborhood(self, lat, lon):
        # Reverse geocode the latitude and longitude
        location = geolocator.reverse((lat, lon), exactly_one=True, language='pt-BR')

        if location:
            # Access the raw data
            address = location.raw.get('address', {})

            # Get the neighborhood if available
            neighborhood = address.get('neighbourhood', None)

            if neighborhood:
                return neighborhood
            else:
                return ""
        else:
            return ""

    def add_new_column_data(self, df):
        df['BAIRRO'] = ''
        df['CEP'] = ''
        for i in range(len(df)):
            row_data = df[df['lat'] == df['lat'].value_counts().index[i]].iloc[i]
            # df.at[i, 'RUA'] = self.get_postcode_from_lat_and_lon(row_data['lat'], row_data['lon'])
            df.at[i, 'BAIRRO'] = self.get_neighborhood(row_data['lat'], row_data['lon'])
            df.at[i, 'CEP'] = self.get_postcode_from_lat_and_lon(row_data['lat'], row_data['lon'])
        return df

    # def add_CEP_in_df(self, df):
    #     my_map = {}
    #     range_df = range(len(df))
    #     for i in range_df:
    #         row_data = df[df['lat'] == df['lat'].value_counts().index[i-1]].iloc[i-1]
    #         my_map['foco_id'] = row_data['foco_id']
    #         my_map['CEP'] = self.get_postcode_from_lat_and_lon(row_data['lat'], row_data['lon'])
    #     df['CEP'] = df['foco_id'].map(my_map)

    def add_CEP_in_df(self, df):
        df['CEP'] = 'NONE'  # Initialize 'CEP' column with 'NONE'

        # Iterate through each row in the dataframe using iterrows
        for index, row_data in df.iterrows():
            lat = row_data['lat']
            lon = row_data['lon']

            # Get the postcode based on lat and lon
            postcode = self.get_postcode_from_lat_and_lon(lat, lon)

            # Update the 'CEP' column for the current row
            df.at[index, 'CEP'] = postcode if postcode else 'NONE'

        return df

    def add_BAIRRO_in_df(self, df):
        df['BAIRRO'] = 'NONE'  # Initialize 'CEP' column with 'NONE'

        # Iterate through each row in the dataframe using iterrows
        for index, row_data in df.iterrows():
            lat = row_data['lat']
            lon = row_data['lon']

            # Get the postcode based on lat and lon
            postcode = self.get_neighborhood(lat, lon)

            # Update the 'CEP' column for the current row
            df.at[index, 'BAIRRO'] = postcode if postcode else 'NONE'

        return df

    # def add_new_column_data(self, df):
    #     for i in range(len(df)):
    #         # Access the ith row directly instead of filtering
    #         row_data = df[df['lat'] == df['lat'].value_counts().index[i]].iloc[i]
    #
    #         # Call the method to get the postcode
    #         postcode = self.get_postcode_from_lat_and_lon(row_data['lat'], row_data['lon'])
    #
    #         # Check if the postcode is valid
    #         if postcode:
    #             df.at[i, 'CEP'] = postcode
    #         else:
    #             df.at[i, 'CEP'] = 'NONE'
    #
    #     return df

    def set_dataframe_for_city(self, dataframe, city):
        df_city = dataframe[dataframe['municipio'] == city]
        print(f'{city} count: {df_city.count()}')
        self.add_CEP_in_df(df_city)
        self.add_BAIRRO_in_df(df_city)
        return df_city
        # top_20_values = dataframe['estado'].value_counts().head(20).index
        # print(self.get_column_types(dataframe))
        # print(dataframe[dataframe['lat'] == dataframe['lat'].value_counts().index[1]].iloc[1])
        # for data in dados_queimadas['lat'].value_counts().head(20):
        #     most_frequent_value = data.index[0]
        #     most_frequent_count = data.iloc[0]
        #     # Display the result
        #     print(f"Most frequent value: {most_frequent_value}, Count: {most_frequent_count}")

    # Function to apply H3 spatial indexing at a resolution level appropriate for max_distance_km
    def add_h3_index(self, df, resolution):
        df['h3_index'] = df.apply(lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], resolution), axis=1)
        df['hex_area_km2'] = df['h3_index'].apply(lambda h3_index: h3.cell_area(h3_index, unit='km^2'))
        return df

    # Function to apply DBSCAN on each H3 hex cell cluster
    def cluster_h3_cell(self, points, max_distance_km):
        coords = points[['latitude', 'longitude']].to_numpy()
        kms_per_radian = 6371.0088
        epsilon = max_distance_km / kms_per_radian  # Convert km to radians for haversine metric
        db = DBSCAN(eps=epsilon, min_samples=2, algorithm='ball_tree', metric='haversine').fit(np.radians(coords))
        points['cluster'] = db.labels_
        cluster_labels = db.labels_
        return points

    def run_dbscan(self, df, distance_in_kilometers):
        # Run DBSCAN within each H3 cell in parallel using Dask
        clustered_dfs = []
        for h3_index, points in df.groupby('h3_index'):
            delayed_cluster = delayed(self.cluster_h3_cell)(points, distance_in_kilometers)  # No need to pass h3_index
            clustered_dfs.append(delayed_cluster)

        # Compute clusters and combine results
        clustered_results = compute(*clustered_dfs)
        return pd.concat(clustered_results)

    def find_nearest_points_in_valid_cluster(self, df):
        # Filtrar apenas os clusters válidos (cluster >= 0)
        df_valid_clusters = df[df['cluster'] >= 0]

        # Função para processar um pedaço do dataframe
        def process_chunk(chunk):
            features = []
            for _, row in chunk.iterrows():
                # Criar o ponto com longitude e latitude
                point = geojson.Point((row['longitude'], row['latitude']))

                # Extrair todas as colunas do DataFrame como propriedades, exceto latitude e longitude
                properties = row.drop(['latitude', 'longitude']).to_dict()
                properties['latitude'] = row['latitude']
                properties['longitude'] = row['longitude']

                # Criar a Feature com geometria e propriedades
                feature = geojson.Feature(geometry=point, properties=properties)
                features.append(feature)
            return features

        # Dividir o DataFrame em partes menores para paralelizar
        ddf = dd.from_pandas(df_valid_clusters, npartitions=4)  # Ajuste o número de partições conforme necessário
        delayed_results = ddf.map_partitions(lambda df: delayed(process_chunk)(df))

        # Computar os resultados
        features_lists = compute(*delayed_results)

        # Juntar todos os recursos em uma única lista
        all_features = [feature for sublist in features_lists for feature in sublist]

        # Criar o objeto GeoJSON com todos os recursos
        return geojson.FeatureCollection(all_features)

    def export_geojson_for_valid_cluster(self, df, max_distance_km, resolution, file_name):
        df_sorocaba = self.add_h3_index(df, resolution=resolution)
        clustered_df = self.run_dbscan(df_sorocaba, max_distance_km)
        return clustered_df

    def set_geo_json_file(self, clustered_df, file_name):
        geojson_data = self.find_nearest_points_in_valid_cluster(clustered_df)
        with open(file_name + '.geojson', 'w') as f:
            geojson.dump(geojson_data, f)

    def find_nearest_points(self, df):
        nearest_points = []

        # Itera sobre cada cluster
        for cluster_id, points in df.groupby('cluster'):
            # Calcula o centroide geográfico do cluster
            centroid_lat = points['latitude'].mean()
            centroid_lon = points['longitude'].mean()
            centroid = (centroid_lat, centroid_lon)

            # Adiciona uma coluna com a distância do ponto ao centroide
            points['distance_to_centroid_km'] = points.apply(
                lambda row: geodesic((row['latitude'], row['longitude']), centroid).kilometers, axis=1
            )

            # Encontra o ponto mais próximo do centroide
            nearest_point = points.loc[points['distance_to_centroid_km'].idxmin()]
            nearest_points.append(nearest_point)

        # Retorna os pontos mais próximos de cada cluster em um DataFrame
        return pd.DataFrame(nearest_points)

    # Function to convert clusters to GeoJSON
    def df_to_geojson(self, df, h3_column, cluster_column):
        features = []
        for h3_index, group in df.groupby(h3_column):
            cluster_ids = group[cluster_column].unique()
            # Convert H3 hex to polygon coordinates
            hex_boundary = h3.cell_to_boundary(h3_index)
            polygon = Polygon(hex_boundary)
            # Create GeoJSON feature for each hexagon with cluster ids
            feature = {
                "type": "Feature",
                "geometry": json.loads(gpd.GeoSeries([polygon]).to_json())['features'][0]['geometry'],
                "properties": {
                    "h3_index": h3_index,
                    "clusters": list(cluster_ids)
                }
            }
            features.append(feature)
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        return geojson

    # Convert to GeoJSON format with each point as a Feature
    def all_points_to_geojson(self, df, h3_column, cluster_column):
        features = []
        for idx, row in df.iterrows():
            # Create a Point geometry for each row
            point = Point(row['longitude'], row['latitude'])
            feature = {
                "type": "Feature",
                "geometry": json.loads(gpd.GeoSeries([point]).to_json())['features'][0]['geometry'],
                "properties": {
                    "latitude": row['latitude'],
                    "longitude": row['longitude'],
                    "h3_index": row[h3_column],
                    "cluster": row[cluster_column]
                }
            }
            features.append(feature)
        geojson = {
            "type": "FeatureCollection",
            "features": features
        }
        return geojson

    def fileExist(self, pathToFIle):
        return os.path.isfile(pathToFIle)

    def export_excel_with_sheets(self, dataframes):
        file_path = 'output.xlsx'

        # Verificar se o arquivo já existe
        if os.path.exists(file_path):
            # Se existir, usar o modo append para adicionar novas planilhas
            with pd.ExcelWriter(file_path, mode='a', if_sheet_exists='new') as writer:
                for sheet_name, df in dataframes.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        else:
            # Se não existir, criar o arquivo em modo de escrita (write)
            with pd.ExcelWriter(file_path, mode='w') as writer:
                for sheet_name, df in dataframes.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

    def save_cluster_points_images(self, city_name):
        # Carregar o GeoJSON em um GeoDataFrame
        gdf = gpd.read_file(city_name + '.geojson')
        print(gdf.head())  # Verifique se o gdf foi carregado corretamente

        # Configurar o sistema de coordenadas para o gdf e adicionar o mapa base
        gdf = gdf.to_crs(epsg=3857)  # CRS para coordenadas compatíveis com o contexto do mapa

        # Plotar o mapa
        fig, ax = plt.subplots(figsize=(30, 30))  # Reduzido o tamanho para evitar problemas de memória
        gdf.plot(ax=ax, color='red', marker='o', markersize=20, alpha=0.7, label="Pontos de Cluster")

        # Adicionar um mapa base usando o provedor "OSM" (OpenStreetMap)
        ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik, crs=gdf.crs.to_string())

        # Ajustar a visualização e título
        ax.set_title("Mapa com Pontos de Cluster GeoJSON", fontsize=15)
        plt.legend()

        # Salvar o mapa em imagem
        plt.savefig("mapa_cluster_" + city_name + ".png", dpi=300)
        plt.close(fig)  # Fecha o plot para liberar memória