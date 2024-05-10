#  mapa_resgate_enchente
Mapa que plota os pedidos de resgate (enchente Porto Alegre e região - Maio de 2024) 

Esse repositório guarda o script que gera o HTML do mapa com os pedidos de resgate (enchente Porto Alegre e região - Maio de 2024).

O mapa pode ser acessado em: http://www.enchente.info/

Também há duas outras versões do mapa de pedidos de resgate:
- www.enchente.info/mapa_hoje.html : mostra somente os pedidos feitos no dia atual.
- www.enchente.info/mapa_24h.html : mostra somente os pedidos feitos nas últimas 24h.

O mapa pega as informações de pedido de resgate da tabela em https://docs.google.com/spreadsheets/d/1JD5serjAxnmqJWP8Y51A6wEZwqZ9A7kEUH1ZwGBx1tY/edit?usp=sharing.

- Formulário de resgate: https://forms.gle/SiuM24DpZDj7orfKA
- Formulário de resgatados: https://forms.gle/s57RTYnAYK2hC5sc8

## Instalação
pip3 install -r requirements.txt

## Uso
python3 mapa_resgate_enchente/atualiza_mapa.py

Esse script busca os dados de duas tabelas de pedidos de resgate e plota eles no mapa.
Os pedidos tidos como encerrados/resolvidos não são plotados.
A cada atualização, as coordenadas de cada pedido são calculadas e salvas em um arquivo CSV, para que não seja preciso buscar de novo na próxima atualização.

Atualmente, temos uma VM que roda um cronjob o script atualiza_mapa.py a cada 5min, atualizando o mapa que ta no ar.

## TODOs
- Arrumar alguns endereços que aparecem em locais errados.
