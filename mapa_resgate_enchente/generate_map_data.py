#mapa resgate script 2024_06_05_0
from difflib import SequenceMatcher
import pandas as pd
import requests
from io import StringIO
import sys
import os
from geopy.geocoders import Photon
from io import BytesIO
from datetime import datetime
import geopy
from typing import Tuple
from generate_html import generate_html
from paths import DF_SHEETS_FILEPATH, DF_GABINETE_FILEPATH, DF_WITHOUT_COORDS_FILEPATH, DF_UNMAPPED_FILEPATH, DF_MAPPED_FILEPATH, MAPPED_BACKUPS_FOLDERPATH

GEOLOCATOR = Photon(user_agent="measurements")

# parameters
URL_DADOS_GABINETE = 'https://onedrive.live.com/download?resid=C734B4D1CCD6CEA6!94437&authkey=!ABnn6msPt2x5OFk'
IDENTIFIER_COLUMNS = ["DATAHORA", "NUMPESSOAS", "DETALHES", "LOGRADOURO", "CONTATORESGATADO", 
                      "DESCRICAORESGATE", "NUM","COMPLEMENTO","BAIRRO","CIDADE"]
DEBUG = False # pra rodar mais rapido, soh com 10 rows, pra debug


def get_place_id(input_text: str, api_key: str) -> str:
    endpoint_url = "https://maps.googleapis.com/maps/api/place/autocomplete/json"
    params = {
        'input': input_text,
        'key': api_key
    }
    try:
        response = requests.get(endpoint_url, params=params)
        place_id = response.json()['predictions'][0]['place_id']
        return place_id
    except:
        return False

def get_location(place_id: str, api_key: str) -> Tuple[float, float]:
    details_url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        'place_id': place_id,
        'fields': 'geometry',
        'key': api_key
    }
    try:
        response = requests.get(details_url, params=params)
        location = response.json()['result']['geometry']['location']
        return location['lat'], location['lng']
    except:
        return False, False

def similar(a: str, b: str):
    return SequenceMatcher(None, a, b).ratio()

def similarity_value(x: pd.core.series.Series, location: geopy.location.Location):
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

def get_coords(row: pd.core.series.Series):
    address = row['address']
    place_id = get_place_id(address, api_key)
    if place_id:
        latitude, longitude = get_location(place_id, api_key)
        if latitude and longitude:
            return [latitude, longitude, "1"] # Attempt to extract the ZIP code
        else:
            print(f"Failed to find a place like: {address}")
            return ["","","0"]            
    else:
        print(f"Failed to fetch the coordinates for: {address}")
        return ["","","0"]
    
def get_coords_df(df_sheets: pd.DataFrame):
    print(f"Getting coordinates for {len(df_sheets)} addresses...")
    df = df_sheets.copy()
    df["address"] = df["LOGRADOURO"] + "," + df["NUM"] + ", " + df["BAIRRO"]  + ", " + df["CIDADE"]
    outs = []
    L = len(df)
    idx = 0
    for index, row in df.iterrows():
        if index % 5==0:
            print("row {}/{}".format(idx,L)) #print current step
        out = get_coords(row)
        outs.append(out)
        idx += 1
        
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
        df = pd.read_csv(csv_data, sep=",", dtype=str)
        print(f"Fetched {len(df)} rows from LAGON Google Sheets")
    else:
        print(f"Requisicao dos dados do Google Sheet falhou com erro {response.status_code}")
        sys.exit(1)
    return df

def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.iloc[0]
    #renomear colunas para evitar incompatibilidades com o sheet
    cols[0:14] = ['DATAHORA','NUMPESSOAS','DETALHES','LOGRADOURO','CONTATORESGATADO','DESCRICAORESGATE',
              'NUM','COMPLEMENTO','BAIRRO','CIDADE','CEP','NOMEPESSOAS','CADASTRADO','ENCERRADO']
    named_cols = [c for c in cols if len(c)>0]
    df.columns = cols
    df = df[named_cols]
    df = df.iloc[1:]
    return df

def get_df_sheets() -> pd.DataFrame:
    # get data from google sheets
    df_sheets = get_google_sheet(spreadsheet_id="1JD5serjAxnmqJWP8Y51A6wEZwqZ9A7kEUH1ZwGBx1tY")
    df_sheets = prepare_dataframe(df_sheets) 
    df_sheets = df_sheets[df_sheets['LOGRADOURO'].notna()]
    df_sheets["len"] = df_sheets["LOGRADOURO"].apply(lambda x : len(x))
    df_sheets = df_sheets[df_sheets["len"]>0]
    df_sheets = df_sheets[df_sheets["ENCERRADO"]!="S"]
    df_sheets = df_sheets.drop("len",axis = 1)
    print(f"Fetched {len(df_sheets)} rows from LAGON")
    return df_sheets

def get_df_gabinete() -> pd.DataFrame:
    response = requests.get(URL_DADOS_GABINETE)
    assert response.status_code == 200, "Erro ao baixar o arquivo"
    # Usando pandas para ler os dados da planilha
    df_gabinete = pd.read_excel(BytesIO(response.content))
    df_gabinete = df_gabinete[df_gabinete["ENCERRADO"]!="S"]
    print(f"Fetched {len(df_gabinete)} rows from GABINETE")
    return df_gabinete

def process_df_gabinete(df_gabinete: pd.DataFrame) -> pd.DataFrame:
    """ deixa as colunas iguais a df_sheets
    """
    # df_sheets.columns:
    # ['DATAHORA', 'DESCRICAORESGATE', 'DETALHE', 'LOGRADOURO',
    # 'CONTATORESGATADO', 'INFORMACOES', 'NUM', 'COMPL', 'BAIRRO', 'CIDADE',
    # 'CEP', 'NOMEPESSOAS', 'CADASTRADO', 'ENCERRADO'],
    # df_gabinete.columns:
    # ['Unnamed: 0', 'PRIORIDADES', 'Bairro', 'OBSERVAÇÃO', 'CONTATO', 'OBS',
    # 'RESGATADOS ', 'Unnamed: 7'],
    df_gabinete.rename(columns={"Bairro": "BAIRRO",
                                "OBSERVAÇÃO": "DESCRICAORESGATE",
                                "CONTATO": "CONTATORESGATADO"
                                }, inplace=True)
    df_gabinete["ADDRESS"] = df_gabinete.iloc[:, 0] + " " + df_gabinete["PRIORIDADES"]
    # ADDRESS = LOGRADOURO + NUMERO + TUDO
    df_gabinete["LOGRADOURO"] = df_gabinete["ADDRESS"]
    df_gabinete["NUM"] = ""
    df_gabinete["COMPL"] = ""
    df_gabinete["CIDADE"] = ""
    df_gabinete["DETALHE"] = ""
    df_gabinete["CEP"] = ""
    df_gabinete["INFORMACOES"] = ""
    df_gabinete["NOMEPESSOAS"] = ""
    df_gabinete["CADASTRADO"] = ""
    df_gabinete.drop(axis=1, labels=['Unnamed: 0', 'Unnamed: 7', 'PRIORIDADES'], inplace=True)
    return df_gabinete

def get_df_unmapped(df_previous: pd.DataFrame, df_without_coords : pd.DataFrame) -> pd.DataFrame:
    df_unmapped = pd.merge(df_without_coords, df_previous[IDENTIFIER_COLUMNS+["success","latitude","longitude"]], on = IDENTIFIER_COLUMNS, how = "left")
    df_unmapped = df_unmapped[df_unmapped["success"]!="1"]
    df_unmapped = df_unmapped[list(df_without_coords.columns)]
    return df_unmapped

def get_df_with_coordinates(df_without_coords: pd.DataFrame) -> pd.DataFrame:

    if not os.path.exists(DF_MAPPED_FILEPATH):
        df = get_coords_df(df_without_coords)
        df.to_csv(DF_MAPPED_FILEPATH, index = False)
        print(f"Saved {DF_MAPPED_FILEPATH}")
        #collect info from unmapped (required to save unmapped values)
        df_previous = pd.read_csv(DF_MAPPED_FILEPATH, dtype = str)
        df_unmapped = get_df_unmapped(df_previous, df_without_coords)
    else:
        df_previous = pd.read_csv(DF_MAPPED_FILEPATH, dtype = str)
        len0 = len(df_previous)
        df_unmapped = get_df_unmapped(df_previous, df_without_coords)
        df = get_coords_df(df_unmapped) # DEBUG
        df = pd.concat([df,df_previous])
        
        # save DataFrame with coordinates locally
        if len(df)>len0:
            df = df.drop_duplicates(IDENTIFIER_COLUMNS)
            df.to_csv(DF_MAPPED_FILEPATH, index = False)
            print(f"Saved {DF_MAPPED_FILEPATH}")

    num_mapped = len(df)
    num_unmapped = len(df_unmapped)
    print(f"num_mapped: {num_mapped}, num_unmapped: {num_unmapped}")

    #update unmapped rows
    df_previous = pd.read_csv(DF_MAPPED_FILEPATH, dtype = str)
    df_unmapped = pd.merge(df_without_coords, df_previous[IDENTIFIER_COLUMNS + ["success","latitude","longitude"]], on = IDENTIFIER_COLUMNS, how = "left")
    df_unmapped = df_unmapped[df_unmapped["success"]!="1"]
    df_unmapped = df_unmapped[list(df_without_coords.columns)]
    df_unmapped.to_csv(DF_UNMAPPED_FILEPATH)
    return df, df_unmapped


# TODO limpar/organizar essa funcao
def generate_map_data(debug: bool) -> Tuple[pd.DataFrame, bool]:
    """
    Retorna:
        - df_mapped : pd.DataFrame
            TODO
        - has_map_data_changed : bool
            TODO

    E salva:
        - df_sheets.csv
            TODO
        - df_gabinete.csv
            TODO
        - df_mapped.csv
            TODO
        - df_unmapped.csv
            TODO
    """

    df_sheets = get_df_sheets()
    df_gabinete = get_df_gabinete()
    df_gabinete = process_df_gabinete(df_gabinete)

    # save CSVs 
    df_sheets.to_csv(path_or_buf=DF_SHEETS_FILEPATH)
    print(f"Saved {DF_SHEETS_FILEPATH}")
    df_gabinete.to_csv(path_or_buf=DF_GABINETE_FILEPATH)
    print(f"Saved {DF_GABINETE_FILEPATH}")

    # merge data from LAGOM and GABINETE sources:
    df_without_coords = pd.concat([df_sheets, df_gabinete])

    # save CSV before getting coordinates
    df_without_coords.to_csv(path_or_buf=DF_WITHOUT_COORDS_FILEPATH)
    print(f"Saved {DF_WITHOUT_COORDS_FILEPATH}")

    if debug:
        # pra rodar mais rapido
        df_without_coords = df_without_coords.iloc[0:50]

    # save CSV before getting coordinates
    df_without_coords.to_csv(path_or_buf=DF_SHEETS_FILEPATH)
    print(f"Saved {DF_SHEETS_FILEPATH}")

    # pegar coordenadas
    df, df_unmapped = get_df_with_coordinates(df_without_coords=df_without_coords)

    # ultimo tratamento
    # TODO organizar numa funcao
    df_mapped = pd.read_csv(DF_MAPPED_FILEPATH, dtype = str)
    print(f"Loaded {DF_MAPPED_FILEPATH}")

    # treat NaN values
    df_mapped = df_mapped[df_mapped["latitude"].notna()]

    #remove ENCERRADOS
    print("Removing ENCERRADO")
    print("Before removal: {} rows".format(len(df_mapped)))
    df_mapped = pd.merge(df_without_coords, df_mapped[IDENTIFIER_COLUMNS + ["success","latitude","longitude"]], 
                  on = IDENTIFIER_COLUMNS, how = "left")
    df_mapped = df_mapped[df_mapped["success"]=="1"]
    df_mapped = df_mapped[df_mapped["ENCERRADO"]!="S"]
    print("After removal: {} rows".format(len(df_mapped)))

    # check if map data has changed since last update
    os.makedirs(MAPPED_BACKUPS_FOLDERPATH, exist_ok=True)
    backup_filepath_list = os.listdir(MAPPED_BACKUPS_FOLDERPATH)
    backup_filepath_list = list(filter(lambda x: x[-4:] == '.csv', backup_filepath_list))
    if len(backup_filepath_list) > 0:
        last_backup_filepath = MAPPED_BACKUPS_FOLDERPATH / sorted(backup_filepath_list)[-1]
        last_df_mapped_backup = pd.read_csv(last_backup_filepath, dtype = str)
        has_map_data_changed = not df_mapped.fillna('').reset_index(drop=True).eq(last_df_mapped_backup.fillna('').reset_index(drop=True)).all().all()
    else:
        last_backup_filepath = " "
        has_map_data_changed = True

    # save df_mapped backup if map data has changed 
    if has_map_data_changed:
        now = datetime.now() # current date and time
        format = "%Y_%m_%d-%H_%M_%S"
        timestamp = now.strftime(format)
        backup_csv_filepath = MAPPED_BACKUPS_FOLDERPATH / f"backup_df_mapped_{timestamp}.csv"
        df_mapped.to_csv(path_or_buf=backup_csv_filepath, index=False)
        print(f"Saved {backup_csv_filepath}")
    else:
        print(f"Nothing changed since last backup: {last_backup_filepath}")

    return df_mapped, has_map_data_changed

