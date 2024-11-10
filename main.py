import json
import math
from io import BytesIO
from pathlib import Path

import geojson
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import openpyxl as xl
import requests
from PIL.Image import Image
from dask import compute
import math
import os
from pathlib import Path
from typing import List, Dict, Hashable
import h3
from sklearn.cluster import DBSCAN
from dask import delayed, compute
from dask.distributed import Client
from sklearn.metrics.pairwise import haversine_distances
from sklearn.metrics.pairwise import euclidean_distances

from utilsClass import Utils
from geopy.geocoders import Nominatim
import geopandas as gpd
from shapely.geometry import Point
import contextily as ctx

utils = Utils("test");
# # notas=np.array([9,8,5,7])
# # print(notas[idades==20])
# # print(np.mean(idades*0.4))
# #
# # tabela=pd.DataFrame({'nome':['william', 'joao', 'rafael', 'maria'], 'nota':[10,7,8,10]})
# # print(tabela.nota[tabela.nome=="william"])
#
# dados_pessoas = pd.read_excel('dados.xlsx', sheet_name='pessoas')
# dados_notas = pd.read_excel('dados.xlsx', sheet_name='notas')
#
# print(dados_pessoas)
# print(dados_notas)
#
# dados_todos = dados_notas.set_index('nome').join(dados_pessoas.set_index('nome'))
#
# print(dados_todos)
#
# #definir media de notas dos alunos
# medias = dados_todos.groupby('nome').nota.mean()
# print(medias)

# # fig, simple_chart = plt.subplots()
# nomes_test = ['william', 'jaque', 'gigi', 'gabi']
# idades=np.array([38,36,12,6])
# plt.plot(nomes_test, idades)
# plt.xlabel('nomes')
# plt.ylabel('idades')
# plt.show()
# plt.bar(nomes_test, idades)
# plt.xlabel('nomes')
# plt.ylabel('idades')
# plt.show()

initial_year = 2003

dados_queimadas_total = utils.create_data_frame_array('./base_de_dados_inpe/focos_br_sp_ref_', initial_year, ',', 21)
dados_queimadas = utils.append_all_dataframes(dados_queimadas_total)


# df_sorocaba = utils.set_h3_index(df_sorocaba)
# clustered_dfs = utils.run_dbscan(df_sorocaba, 5)
# # Compute clusters and combine results
# clustered_results = compute(*clustered_dfs)
# clustered_df = pd.concat(clustered_results)
# print(clustered_df[['latitude', 'longitude', 'h3_index', 'cluster']])

# Sample DataFrame
max_distance_km = 0.5
# Add H3 index to DataFrame
resolution = 9  # Adjust based on required precision; higher resolution means smaller hexes

sorocaba = 'SOROCABA'
votorantim = 'VOTORANTIM'
ipero = 'IPERÓ'
piedade = 'PIEDADE'
tapirai = 'TAPIRAÍ'
pilar_do_sul = 'PILAR DO SUL'
salto_de_pirapora = 'SALTO DE PIRAPORA'
sao_miguel_arcanjo = 'SÃO MIGUEL ARCANJO'

df_votorantim = dados_queimadas[dados_queimadas['municipio'] == votorantim]
df_votorantim = df_votorantim.rename(columns = {'lat':'latitude','lon':'longitude'})
df_votorantim = utils.export_geojson_for_valid_cluster(df_votorantim, max_distance_km, resolution, votorantim)
utils.set_geo_json_file(df_votorantim, votorantim)
utils.save_cluster_points_images(votorantim)

# df_sorocaba = dados_queimadas[dados_queimadas['municipio'] == sorocaba]
# df_sorocaba = df_sorocaba.rename(columns = {'lat':'latitude','lon':'longitude'})
# df_sorocaba = utils.export_geojson_for_valid_cluster(df_sorocaba, max_distance_km, resolution, sorocaba)
# utils.set_geo_json_file(df_sorocaba, sorocaba)
# utils.save_cluster_points_images(sorocaba)
#
# df_piedade = dados_queimadas[dados_queimadas['municipio'] == piedade]
# df_piedade = df_piedade.rename(columns = {'lat':'latitude','lon':'longitude'})
# df_piedade = utils.export_geojson_for_valid_cluster(df_piedade, max_distance_km, resolution, piedade)
# utils.set_geo_json_file(df_piedade, piedade)
# utils.save_cluster_points_images(piedade)
#
# df_tapirai = dados_queimadas[dados_queimadas['municipio'] == tapirai]
# df_tapirai = df_tapirai.rename(columns = {'lat':'latitude','lon':'longitude'})
# df_tapirai = utils.export_geojson_for_valid_cluster(df_tapirai, max_distance_km, resolution, tapirai)
# utils.set_geo_json_file(df_tapirai, tapirai)
# utils.save_cluster_points_images(tapirai)
#
# df_pilar = dados_queimadas[dados_queimadas['municipio'] == pilar_do_sul]
# df_pilar = df_pilar.rename(columns = {'lat':'latitude','lon':'longitude'})
# df_pilar = utils.export_geojson_for_valid_cluster(df_pilar, max_distance_km, resolution, pilar_do_sul)
# utils.set_geo_json_file(df_pilar, pilar_do_sul)
# utils.save_cluster_points_images(pilar_do_sul)
#
# df_salto = dados_queimadas[dados_queimadas['municipio'] == salto_de_pirapora]
# df_salto = df_salto.rename(columns = {'lat':'latitude','lon':'longitude'})
# df_salto = utils.export_geojson_for_valid_cluster(df_salto, max_distance_km, resolution, salto_de_pirapora)
# utils.set_geo_json_file(df_salto, salto_de_pirapora)
# utils.save_cluster_points_images(salto_de_pirapora)
#
# df_sm_arcanjo = dados_queimadas[dados_queimadas['municipio'] == sao_miguel_arcanjo]
# df_sm_arcanjo = df_sm_arcanjo.rename(columns = {'lat':'latitude','lon':'longitude'})
# df_sm_arcanjo = utils.export_geojson_for_valid_cluster(df_sm_arcanjo, max_distance_km, resolution, sao_miguel_arcanjo)
# utils.set_geo_json_file(df_sm_arcanjo, sao_miguel_arcanjo)
# utils.save_cluster_points_images(sao_miguel_arcanjo)
#
# df_ipero = dados_queimadas[dados_queimadas['municipio'] == ipero]
# df_ipero = df_ipero.rename(columns = {'lat':'latitude','lon':'longitude'})
# df_ipero = utils.export_geojson_for_valid_cluster(df_ipero, max_distance_km, resolution, ipero)
# utils.set_geo_json_file(df_ipero, ipero)
# utils.save_cluster_points_images(ipero)



#
# # Adiciona geometria de ponto para visualização em ferramentas GIS
# nearest_points_df['geometry'] = [Point(xy) for xy in zip(nearest_points_df['longitude'], nearest_points_df['latitude'])]
#
# # Cria o GeoDataFrame
# nearest_points_gdf = gpd.GeoDataFrame(nearest_points_df, geometry='geometry')
# nearest_points_gdf.set_crs("EPSG:4326", inplace=True)
#
# # Exporta para GeoJSON
# nearest_points_gdf.to_file("nearest_points_clusters.geojson", driver="GeoJSON")

# Generate the GeoJSON
# geojson_data = utils.df_to_geojson(clustered_df, h3_column='h3_index', cluster_column='cluster')
# geojson_data = utils.find_nearby_points_in_cluster(clustered_df, max_distance_km)
#
# # Exporta para GeoJSON
# # Save to a .geojson file
# with open("sorocaba_points_clusters.geojson", "w") as f:
#     json.dump(geojson_data, f)
dataframes = {
    # 'Sorocaba': df_sorocaba,
    'Votorantim': df_votorantim,
    # 'Iperó': df_ipero,
    # 'Piedade': df_piedade,
    # 'Tapirai': df_tapirai,
    # 'Pilar do Sul': df_pilar,
    # 'Salto de Pirapora': df_salto,
    # 'São Miguel Arcanjo': df_sm_arcanjo
}
utils.export_excel_with_sheets(dataframes)
# if not utils.fileExist('output_voto.xlsx'):

# Sub-região 3 da Região Metropolitana de Sorocaba
# utils.count_of_data(dados_queimadas, 'IPERÓ')
# utils.count_of_data(dados_queimadas, 'PIEDADE')
# utils.count_of_data(dados_queimadas, 'PILAR DO SUL')
# utils.count_of_data(dados_queimadas, 'SALTO DE PIRAPORA')
# utils.count_of_data(dados_queimadas, 'SÃO MIGUEL ARCANJO')
# utils.count_of_data(dados_queimadas, 'SOROCABA')
# utils.count_of_data(dados_queimadas, 'TAPIRAÍ')
# utils.count_of_data(dados_queimadas, 'VOTORANTIM')


# print(utils.get_column_types(dados_quality_sorocaba))
# print(dados_clima_sorocaba)
# print(dados_clima_sorocaba[dados_clima_sorocaba.isnull().T.any()].count())

# Initialize the geocoder
geolocator = Nominatim(user_agent="my_geocoder")

# Get location from an address
# location = geolocator.reverse(('-23,603', '-47,354'), exactly_one=True, language='pt-BR')
#
# # Display the latitude and longitude
# print(location.raw)

# cep = location.raw['address']['postcode']
# # Construct the URL for the ViaCEP API
# url = f"https://viacep.com.br/ws/{cep}/json/"
#
# # Make the request
# response = requests.get(url)
#
# # Check if the request was successful
# if response.status_code == 200:
#     # Parse the JSON response
#     data = response.json()
#
#     # Check if the CEP was found
#     if 'erro' not in data:
#         street = data.get('logradouro', 'Street not found')
#         neighborhood = data.get('bairro', 'Neighborhood not found')
#         city = data.get('localidade', 'City not found')
#         state = data.get('uf', 'State not found')
#
#         # Display the information
#         print(f"Rua: {street}")
#         print(f"Bairro: {neighborhood}")
#         print(f"Cidade: {city}")
#         print(f"Estado: {state}")
#     else:
#         print("CEP not found.")
# else:
#     print("Error fetching data:", response.status_code)

# # OpenStreetMap static map URL
# map_url = f"https://staticmap.openstreetmap.org/staticmap.php?center={lat},{lon}&zoom=13&size=600x400&markers=color:red%7Clabel:A%7C{lat},{lon}"
#
# # Fetch the image
# response = requests.get(map_url)
#
# # Check if the request was successful
# if response.status_code == 200:
#     # Display the map image using Matplotlib
#     img_array = np.asarray(bytearray(response.content), dtype=np.uint8)
#     img = plt.imread(img_array, format='png')
#     plt.imshow(img)
#     plt.axis('off')  # Turn off axis
#     plt.show()
# else:
#     print("Error fetching map image:", response.status_code)


# colunas_cliente = dados_queimadas_2023.columns
# for x in colunas_cliente:
#     print(f'{x}')

# dados_sorocaba = utils.filter_dataframe_column(dados_queimadas_2023, "municipio", "SOROCABA")
# print(f'Quantidade de dados de queimadas em Sorocaba: \n{dados_sorocaba.count()}\nDados de Sorocaba: \n{dados_sorocaba}\n')
# print(f'Dados de Sorocaba data_pas: \n{dados_sorocaba.data_pas}\n')
# print(f'Dados de Sorocaba foco_id: \n{dados_sorocaba.foco_id}\n')
# print(f'Dados de Sorocaba lat: \n{dados_sorocaba.lat}\n')
# print(f'Dados de Sorocaba lon: \n{dados_sorocaba.lon}\n')
#
# dados_votorantim = utils.filter_dataframe_column(dados_queimadas_2023, "municipio", "VOTORANTIM")
# print(f'Quantidade de dados de queimadas em Votorantim: \n{dados_votorantim.count()}\nDados de Votorantim: \n{dados_votorantim}\n')
# print(f'Dados de Votorantim data_pas: \n{dados_votorantim.data_pas}\n')
# print(f'Dados de Votorantim foco_id: \n{dados_votorantim.foco_id}\n')
# print(f'Dados de Votorantim lat: \n{dados_votorantim.lat}\n')
# print(f'Dados de Votorantim lon asdasdasdasdasda: \n{dados_votorantim.data_pas}\n')
# # Analise preliminar
#
# print(f'Dados nulos tabela geral: {utils.is_dataframe_null_values(dados_sorocaba)}')
#
#
#
# lat_lon_array = dados_votorantim[['lat', 'lon', 'foco_id']].to_numpy()
# colunas_vot = utils.get_column_types(dados_queimadas_2023)
# print(colunas_vot)
# # Definir o valor do threshold, por exemplo 5 km
# threshold_km = 15.0
#
# # Chamar a função para obter as posições dos pares com distância menor que o threshold
# posicoes_below_threshold = utils.calcular_distancias_abaixo_threshold(lat_lon_array, threshold_km)

# Verificar quantos nomes de clientes duplicados
# print(f'Dados de Votorantim lat duplicados: \n{utils.get_column_values_repeated(dados_votorantim, "lat")} \n')
# print(f'Dados de Votorantim lon duplicados: \n{utils.get_column_values_repeated(dados_votorantim, "lon")} \n')
#
# print(f'Dados de Sorocaba lat duplicados: \n{utils.get_column_values_repeated(dados_sorocaba, "lat")} \n')
# print(f'Dados de Sorocaba lon duplicados: \n{utils.get_column_values_repeated(dados_sorocaba, "lon"):} \n')