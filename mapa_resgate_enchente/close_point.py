# mapa resgate script 2024_06_05_0
import hashlib
import pandas as pd
import os
from paths import (
    DF_LAGON_FILEPATH,
    DF_GABINETE_FILEPATH,
    DF_WITHOUT_COORDS_FILEPATH,
    DF_UNMAPPED_FILEPATH,
    DF_MAPPED_FILEPATH,
    DF_CLOSEDBYMAP_FILEPATH,
    MAPPED_BACKUPS_FOLDERPATH,
)

def apply_md5(str: str) -> str:
    return hashlib.md5(str.encode()).hexdigest()

def close_point(point_hash: str) -> bool:
    point_hash = apply_md5("5 pessoas, sem animais a princípio." + "Avenida Farrapos")
    #carrega dataframe dos pontos mapeados df_mapped.csv
    if not os.path.exists(DF_CLOSEDBYMAP_FILEPATH): # se o arquivo com os pontos deletados a partir do mapa não existir
        headers = pd.read_csv(DF_MAPPED_FILEPATH, nrows=0) # pega o cabeçalho de df_mapped_como modelo
        df_closed_by_map = pd.DataFrame(columns=headers.columns) # e cria um datafram para os pontos concluídos a partir do mapa
    else:
        df_closed_by_map = pd.read_csv(DF_CLOSEDBYMAP_FILEPATH) # senão, carrega o arquivo existente

    df_mapped = pd.read_csv(DF_MAPPED_FILEPATH) # carrega em df_mapped todos os pontos que estão sendo exibidos no mapa
    df_mapped["point_hash"] = (df_mapped['latitude'] + df_mapped['longitude']).apply(apply_md5)
    df_point = df_mapped[df_mapped["point_hash"] == point_hash]
    if len(df_point) > 0:
        df_closed_by_map = pd.concat([df_closed_by_map, df_point], ignore_index=True)
        df_closed_by_map.to_csv(DF_CLOSEDBYMAP_FILEPATH, index=False)
        return True
    else:
        return False
