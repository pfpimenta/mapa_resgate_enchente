#  mapa_resgate_enchente
Mapa que plota os pedidos de resgate (enchente Porto Alegre e região - Maio de 2024) 

Esse repositório guarda o script que gera o HTML do mapa com os pedidos de resgate (enchente Porto Alegre e região - Maio de 2024).

O mapa pode ser acessado em: http://www.enchente.info/

O mapa pega as informações de pedido de resgate da tabela em https://docs.google.com/spreadsheets/d/1JD5serjAxnmqJWP8Y51A6wEZwqZ9A7kEUH1ZwGBx1tY/edit?usp=sharing.

- Formulário de resgate: https://forms.gle/SiuM24DpZDj7orfKA
- Formulário de resgatados: https://forms.gle/s57RTYnAYK2hC5sc8

# TODOs
- Deletar entradas de resgates já feitos
- Arrumar endereços que dão erro ao pedir as coordenadas para a API (Geocoder using Photon geocoding service)
- Arrumar endereços que aparecem em locais errados
- Salvar os dados processados pra aproveitar eles e não ter que reprocessar tudo a cada rodada