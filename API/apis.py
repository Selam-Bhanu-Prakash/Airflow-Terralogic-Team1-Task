from flask import Flask, jsonify, request
from flask_cors import CORS
from airflow.models import Connection
from airflow import settings

app = Flask(__name__)
CORS(app)
with app.app_context():
    response = jsonify({"message": "Response from the API"})
    response.headers.add("Access-Control-Allow-Origin", "http://localhost:3000")
    response.headers.add("Access-Control-Allow-Methods", "POST")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type")

@app.route('/api/create-connection', methods=['POST'])
def create_connection():

    
    
    data = request.get_json()

    conn_id = data.get('conn_id')
    conn_type = data.get('conn_type')
    host = data.get('host')
    login = data.get('login')
    password = data.get('password')
    

    if conn_id and conn_type and host and login and password:
        # Create the Airflow connection
        conn = Connection(conn_id=conn_id, conn_type=conn_type, host=host, login=login, password=password)
        session = settings.Session()
        session.add(conn)
        session.commit()
        session.close()

        return jsonify({'message': f"Connection '{conn_id}' created successfully."}), 200
    else:
        return jsonify({'error': 'Invalid connection parameters.'}), 200

@app.route('/api/get-connections/<conn_id>', methods=['GET'])
def get_connection(conn_id):
    session = settings.Session()
    
    connection = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if connection:
        connection_dict = {
            'conn_id': connection.conn_id,
            'conn_type': connection.conn_type,
            'host': connection.host,
            'login': connection.login,
            'password':connection.password
        }
        session.close()
        return jsonify(connection_dict), 200
    else:
        session.close()
        return jsonify({'error': f"Connection '{conn_id}' not found."}), 404

@app.route('/api/get-connections', methods=['GET'])
def get_all_connections():
    session = settings.Session()
    connections = session.query(Connection).all()
    connection_list = []
    for connection in connections:
        connection_dict = {
            'conn_id': connection.conn_id,
            'conn_type': connection.conn_type,
            'host': connection.host,
            'login': connection.login
        }
        connection_list.append(connection_dict)

    session.close()
    return jsonify({'connections': connection_list}), 200

@app.route('/api/update-connection/<conn_id>', methods=['PUT'])
def update_connection(conn_id):
    data = request.get_json()

    # conn_type = data.get('conn_type')
    # host = data.get('host')
    login = data.get('login')
    password = data.get('password')

    session = settings.Session()
    connection = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if connection:
        if login == connection.login and password == connection.password:
            session.close()
            return jsonify({'message': "Username and Password Already Exists."}), 201
        else:
            print(f'response is{response}')
            connection.login = login or connection.login
            connection.password = password or connection.password

            session.commit()
            session.close()

            return jsonify({'message': f"Connection '{conn_id}' Login Successfully."}), 200
    else:
        session.close()
        return jsonify({'error': f"Connection '{conn_id}' not found."}), 404

if __name__ == '__main__':
    app.run(debug=True)
if __name__ == '__main__':
    app.run(debug=True)



