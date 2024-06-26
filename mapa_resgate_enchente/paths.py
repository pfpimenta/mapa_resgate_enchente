from pathlib import Path

PROJECT_FOLDERPATH = Path(__file__).parent.parent
HTML_BACKUPS_FOLDERPATH = PROJECT_FOLDERPATH / "html_backup"
HTML_BACKUPS_FOLDERPATH.mkdir(parents=True, exist_ok=True)
MAPPED_BACKUPS_FOLDERPATH = PROJECT_FOLDERPATH / "mapped_backup"
MAPPED_BACKUPS_FOLDERPATH.mkdir(parents=True, exist_ok=True)
CSV_DATA_FOLDERPATH = PROJECT_FOLDERPATH / "csv_data"
CSV_DATA_FOLDERPATH.mkdir(parents=True, exist_ok=True)

HTMLMAPA_FILEPATH = PROJECT_FOLDERPATH / "mapa.html"
HTMLMAPA_24h_FILEPATH = PROJECT_FOLDERPATH / "mapa_24h.html"
HTMLMAPA_HOJE_FILEPATH = PROJECT_FOLDERPATH / "mapa_hoje.html"
HTMLINDEX_FILEPATH = PROJECT_FOLDERPATH / "index.html"

DF_LAGON_FILEPATH = CSV_DATA_FOLDERPATH / "df_lagon.csv"
DF_GABINETE_FILEPATH = CSV_DATA_FOLDERPATH / "df_gabinete.csv"
DF_WITHOUT_COORDS_FILEPATH = CSV_DATA_FOLDERPATH / "df_without_coords.csv"
DF_UNMAPPED_FILEPATH = CSV_DATA_FOLDERPATH / "df_unmapped.csv"
DF_MAPPED_FILEPATH = CSV_DATA_FOLDERPATH / "df_mapped.csv"
