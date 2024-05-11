from flask import Flask, jsonify
from close_point import close_point

app = Flask(__name__)

@app.route('/api/baixar_ponto/<point_hash>', methods=['GET'])
def get_baixar_ponto(point_hash):
    result = close_point(point_hash)
    if result:
        data = {"message": "Ocorrência de resgate marcada como concluída!"}
    else:
        data = {"message": "O identificar informado não se refere a nenhum ponto!"}
    return jsonify(data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
