import os
from PIL.Image import Image
from utilsClass import Utils
import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk
import pandas as pd

utils = Utils()
initial_year = 2003
max_distance_km = 0.5
resolution = 8

dados_queimadas_total = utils.create_data_frame_array('./base_de_dados_inpe/focos_br_sp_ref_', initial_year, ',', 21)
dados_queimadas = utils.append_all_dataframes(dados_queimadas_total)
dados_queimadas = dados_queimadas.rename(columns={'lat': 'latitude', 'lon': 'longitude'})
print("Checking if is null data in table: ")
print(utils.is_dataframe_null_values(dados_queimadas))

sorocaba = 'SOROCABA'
votorantim = 'VOTORANTIM'
ipero = 'IPERÓ'
piedade = 'PIEDADE'
tapirai = 'TAPIRAÍ'
pilar_do_sul = 'PILAR DO SUL'
salto_de_pirapora = 'SALTO DE PIRAPORA'
sao_miguel_arcanjo = 'SÃO MIGUEL ARCANJO'

df_sorocaba = utils.set_dataframe_data(dados_queimadas, sorocaba, max_distance_km, resolution)
df_votorantim = utils.set_dataframe_data(dados_queimadas, votorantim, max_distance_km, resolution)
df_ipero = utils.set_dataframe_data(dados_queimadas, ipero, max_distance_km, resolution)
df_piedade = utils.set_dataframe_data(dados_queimadas, piedade, max_distance_km, resolution)
df_tapirai = utils.set_dataframe_data(dados_queimadas, tapirai, max_distance_km, resolution)
df_pilar = utils.set_dataframe_data(dados_queimadas, pilar_do_sul, max_distance_km, resolution)
df_salto = utils.set_dataframe_data(dados_queimadas, salto_de_pirapora, max_distance_km, resolution)
df_sm_arcanjo = utils.set_dataframe_data(dados_queimadas, sao_miguel_arcanjo, max_distance_km, resolution)

dataframes = {
    'Sorocaba': df_sorocaba,
    'Votorantim': df_votorantim,
    'Iperó': df_ipero,
    'Piedade': df_piedade,
    'Tapirai': df_tapirai,
    'Pilar do Sul': df_pilar,
    'Salto de Pirapora': df_salto,
    'São Miguel Arcanjo': df_sm_arcanjo
}

file_name = 'output'
if not os.path.exists(file_name + '.xlsx'):
    utils.export_excel_with_sheets(dataframes, file_name)

utils.ml_data_test(df_sorocaba)
utils.ml_data_test(df_votorantim)
utils.ml_data_test(df_ipero)
utils.ml_data_test(df_piedade)
utils.ml_data_test(df_tapirai)
utils.ml_data_test(df_pilar)
utils.ml_data_test(df_salto)
utils.ml_data_test(df_sm_arcanjo)

cities = {
    "Sorocaba": sorocaba+".png",
    "Votorantim": votorantim+".png",
    "Iperó": ipero+".png",
    "Piedade": piedade+".png",
    "Tapiraí": tapirai+".png",
    "Pilar do Sul": pilar_do_sul+".png",
    "Salto de Pirapora": salto_de_pirapora+".png",
    "São Miguel Arcanjo": sao_miguel_arcanjo+".png"
}

root = tk.Tk()
root.title("Analise de dados Queimadas - Sub-região 3 da Região Metropolitana de Sorocaba")
root.geometry("1800x1600")
notebook = ttk.Notebook(root)
notebook.pack(expand=True, fill="both")

class CityImageTab:
    def __init__(self, city_frame, image_path):
        self.image = Image.open(image_path)
        self.zoom_level = 1.0
        self.photo = ImageTk.PhotoImage(self.image)

        self.canvas = tk.Canvas(city_frame, width=self.photo.width(), height=self.photo.height())
        self.canvas.pack(expand=True)
        self.image_id = self.canvas.create_image(0, 0, anchor="nw", image=self.photo)

        self.canvas.bind("<MouseWheel>", self.zoom)
        self.resize()

    def zoom(self, event):
        if event.delta > 0:
            self.zoom_level *= 1.1  # Zoom in
        elif event.delta < 0:
            self.zoom_level /= 1.1  # Zoom out

        new_width = int(self.image.width * self.zoom_level)
        new_height = int(self.image.height * self.zoom_level)
        resized_image = self.image.resize((new_width, new_height), Image.Resampling.LANCZOS)

        self.photo = ImageTk.PhotoImage(resized_image)
        self.canvas.itemconfig(self.image_id, image=self.photo)
        self.canvas.config(scrollregion=self.canvas.bbox("all"))

    def resize(self):
        width, height = 1800, 1600

        resized_image = self.image.resize((width, height), Image.Resampling.LANCZOS)
        self.photo = ImageTk.PhotoImage(resized_image)

        self.canvas.itemconfig(self.image_id, image=self.photo)
        self.canvas.config(scrollregion=self.canvas.bbox("all"))

def create_city_tab(city, image_path):
    city_frame = ttk.Frame(notebook)
    notebook.add(city_frame, text=city)
    CityImageTab(city_frame, image_path)

def create_excel_tab(excel_path):
    excel_frame = ttk.Frame(notebook)
    notebook.add(excel_frame, text="Excel Data")

    excel_data = pd.ExcelFile(excel_path)
    sheet_names = excel_data.sheet_names

    # Dropdown to select sheet
    selected_sheet = tk.StringVar(value=sheet_names[0])
    sheet_dropdown = ttk.Combobox(excel_frame, textvariable=selected_sheet, values=sheet_names, state="readonly")
    sheet_dropdown.pack(pady=5)

    tree = ttk.Treeview(excel_frame)
    tree.pack(expand=True, fill="both")

    def load_sheet(sheet_name):
        """
        load the selected sheet data into the Treeview
        :param string sheet_name: sheet name
        """
        for col in tree["columns"]:
            tree.heading(col, text="")
            tree.column(col, width=0)
        tree.delete(*tree.get_children())

        dataframe = excel_data.parse(sheet_name)
        tree["columns"] = list(dataframe.columns)
        tree["show"] = "headings"

        for col in dataframe.columns:
            tree.heading(col, text=col)
            tree.column(col, anchor="center", width=120)

        for _, row in dataframe.iterrows():
            tree.insert("", "end", values=list(row))

    load_sheet(sheet_names[0])
    sheet_dropdown.bind("<<ComboboxSelected>>", lambda event: load_sheet(selected_sheet.get()))

for city, image_path in cities.items():
    create_city_tab(city, image_path)

create_excel_tab('output.xlsx')

root.mainloop()