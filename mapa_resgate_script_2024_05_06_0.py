#mapa resgate script 2024_06_05_0
from difflib import SequenceMatcher
import pandas as pd
import folium
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import geopandas as gpd
from folium.plugins import MarkerCluster
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from bs4 import BeautifulSoup
import typing
import requests
import lxml
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from io import StringIO
import sys
import os
from geopy.geocoders import Photon
geolocator = Photon(user_agent="measurements")

# parameters
THIS_FOLDERPATH = os.getcwd()
HTML_FILEPATH =  THIS_FOLDERPATH + "/mapa.html"
DF_SHEETS_FILEPATH = THIS_FOLDERPATH + "/df_sheets.csv"
DF_UNMAPPED_FILEPATH =  THIS_FOLDERPATH + "/df_unmapped.csv"
DF_MAPPED_FILEPATH =  THIS_FOLDERPATH + "/df_mapped.csv"
DEBUG = True # pra rodar mais rapido, soh com 10 rows, pra debug


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def similarity_value(x, location):
    if "city" in location.raw["properties"].keys():
        city = location.raw["properties"]["city"]
    else:
        return False
        
    if "street" in location.raw["properties"].keys():
        street = location.raw["properties"]["street"]
    elif "name" in location.raw["properties"].keys():
        street = location.raw["properties"]["name"]
    else:
        return False
    
    sims = [similar(street, x["LOGRADOURO"]), similar(city, x["CIDADE"])]
    min_sim = min(sims)
    return min_sim
    
def get_coords(row):
    # Geocode the address
    address = row['address']
    locs = geolocator.geocode(address, timeout=1000, location_bias = (-30.0346, -51.2177),  exactly_one=False, limit = 10)
    if not locs:
        print(f"Failed to fetch the coordinates for: {address}")
        return ["","","0"]
    locs = [l for l in locs if "state" in l.raw["properties"].keys()]
    locs = [l for l in locs if l.raw["properties"]["state"] == "Rio Grande do Sul"]
    locs = [l for l in locs if similarity_value(row,l)>=0.75]
    if len(locs)==0:
        print(f"Failed to fetch the coordinates for: {address}")
        return ["","","0"]
    locs = sorted(locs, key = lambda l : similarity_value(row,l), reverse = True)
    location = locs[0]
    return [location.latitude, location.longitude, "1"] # Attempt to extract the ZIP code
    
def get_coords_df(df_sheets):
    print(f"Getting coordinates for {len(df_sheets)} addresses...")
    df = df_sheets.copy()
    df["address"] = df["CIDADE"] + "," + df["LOGRADOURO"] + "," + df["NUM"]
    outs = []
    L = len(df)
    for index, row in df.iterrows():
        if index % 5==0:
            print("row {}/{}".format(index,L)) #print current step
        out = get_coords(row)
        outs.append(out)
        
    lats = [str(o[0]) for o in outs]
    longs =[str(o[1]) for o in outs]
    sucs = [str(o[2]) for o in outs]
    df["latitude"] = lats
    df["longitude"] = longs
    df["success"] = sucs
    df = df[df["success"]=="1"]
    return df
        

#----------------------------------------------------------------------------
# Pull data from sheets -----------------------------------------------------
#----------------------------------------------------------------------------

def get_google_sheet(spreadsheet_id: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv"
    response = requests.get(url)
    if response.status_code == 200:
        csv_data = StringIO(response.content.decode("utf-8"))
        df = pd.read_csv(csv_data, sep=",")
        print(f"Fetched {len(df)} rows from Google Sheets")
    else:
        print(f"Requisicao dos dados do Google Sheet falhou com erro {response.status_code}")
        sys.exit(1)
    return df
# url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSs1ljv88IOv8G8C0L79b2ZZgNxwQVmrkcOJw50rRuZmgMj54fyVPZpCGwg5VsAUp9q5OuxXGTH3-4h/pub?output=csv"
# df = pd.read_csv(url, header=1)  # Usa a segunda linha como cabeçalho

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.iloc[0]
    named_cols = [c for c in cols if len(c)>0]
    df.columns = cols
    df = df[named_cols]
    df = df.iloc[1:]
    return df

def get_df_sheets() -> pd.DataFrame:
    # get data from google sheets
    df_sheets = get_google_sheet(spreadsheet_id="1JD5serjAxnmqJWP8Y51A6wEZwqZ9A7kEUH1ZwGBx1tY")
    df_sheets = prepare_dataframe(df_sheets) 
    # breakpoint()
    # remove rows quando LOGRADOURO eh nulo
    df_sheets["len"] = df_sheets["LOGRADOURO"].apply(lambda x : len(x))
    df_sheets = df_sheets[df_sheets["len"]>0]
    df_sheets = df_sheets[df_sheets["ENCERRADO"]!="S"]
    df_sheets = df_sheets.drop("len",axis = 1)
    return df_sheets

def get_df_with_coordinates(df_sheets: pd.DataFrame) -> pd.DataFrame:
    #FIRST ATTEMPT
    if not os.path.exists(DF_MAPPED_FILEPATH):
        df = get_coords_df(df_sheets)
        df.to_csv(DF_MAPPED_FILEPATH, index = False)
        print(f"Saved {DF_MAPPED_FILEPATH}")

    #REPEAT ATTEMPTS AND INCREASE COORDINATE LIST IF POSSIBLE
    num_attempts = 1
    for t in range(num_attempts):
        df_previous = pd.read_csv(DF_MAPPED_FILEPATH, dtype = str)
        len0 = len(df_previous)
        df_unmapped = pd.merge(df_sheets, df_previous[["DATAHORA","DESCRICAORESGATE","success","latitude","longitude"]], on = ["DATAHORA","DESCRICAORESGATE"], how = "left")
        df_unmapped = df_unmapped[df_unmapped["success"]!="1"]
        df_unmapped = df_unmapped[list(df_sheets.columns)]
        df = get_coords_df(df_unmapped)
        df = pd.concat([df,df_previous])
        df = df.drop_duplicates(["DATAHORA","DESCRICAORESGATE"])

        # save DataFrame with coordinates locally
        if len(df)>len0:
            df.to_csv(DF_MAPPED_FILEPATH, index = False)
            print(f"Saved {DF_MAPPED_FILEPATH}")

    num_mapped = len(df)
    num_unmapped = len(df_unmapped)
    print(f"num_mapped: {num_mapped}, num_unmapped: {num_unmapped}")

    return df, df_unmapped

def generate_html():
    """ gera mapa a partir do arquivo df_mapped.csv
    """

    # Create a map centered around Porto Alegre
    map_porto_alegre = folium.Map(location=[-30.0346, -51.2177], zoom_start=12)

    # Marker cluster
    marker_cluster = MarkerCluster().add_to(map_porto_alegre)

    df = pd.read_csv(DF_MAPPED_FILEPATH, dtype = str)
    print(f"Loaded {DF_MAPPED_FILEPATH}")

    # Add markers to the map
    for idx, row in df.iterrows():
        html = """
        AVISO!
        POR FAVOR VERIFIQUE SE O ENDEREÇO NO MAPA
        CORRESPONDE COM AS INFORMAÇÕES ABAIXO!
        
        Data e hora: {data}<br>

        Cidade: {cidade}<br>
        
        Descrição: {desc}<br>

        Detalhe: {det}<br>
        
        Informações: {info}<br>
        
        Contato: {contato}<br>

        Logradouro: {logradouro}<br>

        Número: {num} <br>

        Complemento: {compl}<br>
        """.format(data = row["DATAHORA"],
                cidade = row["CIDADE"],
                desc = row['DESCRICAORESGATE'],
                det = row["DETALHE"],
                info = row['INFORMACOES'],
                contato = row["CONTATORESGATADO"],
                logradouro = row["LOGRADOURO"],
                num = row["NUM"],
                compl = row["COMPL"]
                )
        lat = row["latitude"]
        long = row["longitude"]
        iframe = folium.IFrame(html)
        popup = folium.Popup(iframe,
                            min_width=500,
                            max_width=500)
        folium.Marker([lat,long], popup=popup).add_to(marker_cluster)

    # save HTML
    map_porto_alegre.save(HTML_FILEPATH)
    print(f"Saved {HTML_FILEPATH}")


    
def main():
    df_sheets = get_df_sheets()

    if DEBUG:
        # pra rodar mais rapido
        df_sheets = df_sheets.iloc[0:5]

    # save CSV before getting coordinates
    df_sheets.to_csv(path_or_buf=DF_SHEETS_FILEPATH)
    print(f"Saved {DF_SHEETS_FILEPATH}")

    # TODO pegar coordenadas ja geradas pra nao ter que pegar de novo

    # pegar coordenadas
    df, df_unmapped = get_df_with_coordinates(df_sheets=df_sheets)

    # criar HTML do mapa
    generate_html()

    # TODO ?
    df_previous = pd.read_csv(DF_MAPPED_FILEPATH, dtype = str)
    # print(f"Loaded {DF_MAPPED_FILEPATH}")
    len0 = len(df_previous)
    df_unmapped = pd.merge(df_sheets, df_previous[["DATAHORA","DESCRICAORESGATE","success","latitude","longitude"]], on = ["DATAHORA","DESCRICAORESGATE"], how = "left")
    df_unmapped = df_unmapped[df_unmapped["success"]!="1"]
    df_unmapped = df_unmapped[list(df_sheets.columns)]

    df_unmapped.to_csv(DF_UNMAPPED_FILEPATH)


if __name__ == "__main__":
    main()




