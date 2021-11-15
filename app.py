from flask_sqlalchemy import *
from flask import Flask, render_template, request
from flask_socketio import SocketIO, join_room
from waitress import serve
import os

app = Flask(__name__)
socketio = SocketIO(app)
socketio = SocketIO(app)


@app.route('/')
def home():
    return render_template('home.html')

@app.route('/search', methods=['POST'])
def search():
    query = request.form['query']
    print(query)
    return render_template('results.html')
if __name__ == '__main__':
    app.config['SESSION_TYPE'] = 'filesystem'
    serve(app,host='192.168.1.18',port=5000)