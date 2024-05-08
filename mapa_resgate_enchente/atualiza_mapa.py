#mapa resgate script 2024_06_05_0
from generate_html import generate_html
from generate_map_data import generate_map_data

DEBUG = False # pra rodar mais rapido, soh com 10 rows, pra debug

    
def main():
    df_mapped, has_map_data_changed = generate_map_data(debug=DEBUG)
    generate_html(df_mapped=df_mapped, has_map_data_changed=has_map_data_changed)

if __name__ == "__main__":
    main()