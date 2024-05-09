import pandas as pd
import folium
from folium.plugins import MarkerCluster
import shutil
from datetime import datetime
import os
from paths import (
    HTML_BACKUPS_FOLDERPATH,
    HTMLMAPA_FILEPATH,
    HTMLINDEX_FILEPATH,
    HTMLMAPA_24h_FILEPATH,
    HTMLMAPA_HOJE_FILEPATH,
)


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


def generate_html_filtered(df_mapped: pd.DataFrame) -> None:
    """gera e salva dois mapas HTML:
    - mapa_24h.html
        contem somente os pedidos registrados nas ultimas 24h
    - mapa_hoje.html
        contem somente os pedidos registrados no dia de hoje
    """
    pass  # TODO


def generate_html(df_mapped: pd.DataFrame, has_map_data_changed: bool = True):
    """gera mapa a partir do DataFrame df_mapped"""
    # Create a map centered around Porto Alegre
    map_porto_alegre = folium.Map(location=[-30.0346, -51.2177], zoom_start=12)

    # Marker cluster
    marker_cluster = MarkerCluster().add_to(map_porto_alegre)

    # Add markers to the map
    for idx, row in df_mapped.iterrows():
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
        """.format(
            data=row["DATAHORA"],
            cidade=row["CIDADE"],
            desc=row["DESCRICAORESGATE"],
            det=row["DETALHE"],
            info=row["INFORMACOES"],
            contato=row["CONTATORESGATADO"],
            logradouro=row["LOGRADOURO"],
            num=row["NUM"],
            compl=row["COMPL"],
        )
        lat = row["latitude"]
        long = row["longitude"]
        iframe = folium.IFrame(html)
        popup = folium.Popup(iframe, min_width=500, max_width=500)
        folium.Marker([lat, long], popup=popup).add_to(marker_cluster)

    # save HTML
    map_porto_alegre.save(HTMLMAPA_FILEPATH)
    print(f"Saved {HTMLMAPA_FILEPATH}")
    map_porto_alegre.save(HTMLINDEX_FILEPATH)
    print(f"Saved {HTMLINDEX_FILEPATH}")

    save_backup_html(has_map_data_changed)
