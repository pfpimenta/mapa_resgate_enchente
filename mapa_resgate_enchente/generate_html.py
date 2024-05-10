import pandas as pd
import folium
from folium.plugins import MarkerCluster
import shutil
from datetime import datetime, timedelta
import os
import math
import hashlib
from paths import (
    HTML_BACKUPS_FOLDERPATH,
    HTMLMAPA_FILEPATH,
    HTMLINDEX_FILEPATH,
    HTMLMAPA_24h_FILEPATH,
    HTMLMAPA_HOJE_FILEPATH,
)


def apply_md5(str: str) -> str:
    return hashlib.md5(str.encode()).hexdigest()

def save_backup_html(has_map_data_changed: bool = True):
    # save backup only if the map data has changed
    os.makedirs(HTML_BACKUPS_FOLDERPATH, exist_ok=True)
    if has_map_data_changed:
        now = datetime.now()  # current date and time
        format = "%Y_%m_%d-%H_%M_%S"
        timestamp = now.strftime(format)
        backup_html_filepath = HTML_BACKUPS_FOLDERPATH / f"backup_mapa_{timestamp}.html"
        shutil.copyfile(HTMLMAPA_FILEPATH, backup_html_filepath)
        print(f"Saved backup {backup_html_filepath}")
    else:
        backup_filepath_list = os.listdir(HTML_BACKUPS_FOLDERPATH)
        backup_filepath_list = list(
            filter(lambda x: x[-5:] == ".html", backup_filepath_list)
        )
        last_backup_filepath = (
            HTML_BACKUPS_FOLDERPATH / sorted(backup_filepath_list)[-1]
        )
        print(f"Nothing changed since last backup {last_backup_filepath}")


def get_html_map(df: pd.DataFrame):
    # Create a map centered around Porto Alegre
    map_porto_alegre = folium.Map(location=[-30.0346, -51.2177], zoom_start=12)

    # Marker cluster
    marker_cluster = MarkerCluster().add_to(map_porto_alegre)
    # Add markers to the map
    for idx, row in df.iterrows():
        if isinstance(row["LOGRADOURO"], str):
            if len(row["LOGRADOURO"]) == 0:
                print(f"LOGRADOURO vazio! row: {str(row.address)}")
        elif math.isnan(row["LOGRADOURO"]):
            print(f"LOGRADOURO vazio! row: {str(row.address)}")
        try:
            html = """
            AVISO!
            POR FAVOR VERIFIQUE SE O ENDEREÇO NO MAPA
            CORRESPONDE COM AS INFORMAÇÕES ABAIXO!

            Data e hora: {data}<br>

            Cidade: {cidade}<br>
            
            Descrição: {desc}<br>

            Detalhe: {det}<br>
                        
            Contato: {contato}<br>

            Logradouro: {logradouro}<br>

            Número: {num} <br>

            Complemento: {compl}<br>

            <a href="http://enchente.info:8080/api/baixar_ponto/{point_hash}">Registrar como resgatado</a>
            """.format(
                data=row["DATAHORA"],
                cidade=row["CIDADE"],
                desc=row["DESCRICAORESGATE"],
                det=row["DETALHES"],
                contato=row["CONTATORESGATADO"],
                logradouro=row["LOGRADOURO"],
                num=row["NUM"],
                compl=row["COMPLEMENTO"],
                point_hash = apply_md5(str(row["latitude"]) + str(row["longitude"])),
            )
        except:
            breakpoint()
        lat = row["latitude"]
        long = row["longitude"]
        iframe = folium.IFrame(html)
        popup = folium.Popup(iframe, min_width=500, max_width=500)
        folium.Marker([lat, long], popup=popup).add_to(marker_cluster)
    return map_porto_alegre

def data_hora_to_datetime(pedido_datahora: str | float) -> datetime:
    format = "%Y-%d/%m %H:%M"
    pedido_datahora = "2024-" + pedido_datahora
    try:
        pedido_timestamp = datetime.strptime(pedido_datahora, format)
    except:
        # soh pra nao mostrar no mapa
        pedido_timestamp = datetime.now() - timedelta(days=2)
    return pedido_timestamp

def generate_html_filtered(df_mapped: pd.DataFrame) -> None:
    """gera e salva dois mapas HTML:
    - mapa_24h.html
        contem somente os pedidos registrados nas ultimas 24h
    - mapa_hoje.html
        contem somente os pedidos registrados no dia de hoje
    """
    today = datetime.today()
    timestamp_24h_ago = datetime.now() - timedelta(hours=24)
    
    df_mapped["timestamp"] = df_mapped["DATAHORA"].apply(func=data_hora_to_datetime)

    df_mapped_today = df_mapped[df_mapped["timestamp"] > today]
    df_mapped_24h_ago = df_mapped[df_mapped["timestamp"] > timestamp_24h_ago]

    html_map_today = get_html_map(df=df_mapped_today)
    html_map_24h_ago = get_html_map(df=df_mapped_24h_ago)

    # save HTMLs
    html_map_today.save(HTMLMAPA_HOJE_FILEPATH)
    print(f"Saved {HTMLMAPA_HOJE_FILEPATH}")
    html_map_24h_ago.save(HTMLMAPA_24h_FILEPATH)
    print(f"Saved {HTMLMAPA_24h_FILEPATH}")



def generate_html_maps(df_mapped: pd.DataFrame, has_map_data_changed: bool = True):
    """gera mapa a partir do DataFrame df_mapped"""

    # generate main map HTML
    map_porto_alegre = get_html_map(df=df_mapped)

    # save HTML
    map_porto_alegre.save(HTMLMAPA_FILEPATH)
    print(f"Saved {HTMLMAPA_FILEPATH}")
    map_porto_alegre.save(HTMLINDEX_FILEPATH)
    print(f"Saved {HTMLINDEX_FILEPATH}")

    save_backup_html(has_map_data_changed)

    # generate maps with only information about last 24h and the today's date
    generate_html_filtered(df_mapped=df_mapped)