from flask import Flask, render_template, request
import json
from inverted_index import *

app = Flask(__name__)
app.secret_key = ".."

@app.route('/')
def home():
    return render_template("home.html")

@app.route('/home')
def home1():
    return render_template("home.html")

@app.route('/reindex')
def reindexpath():
    return render_template("reindex.html")

@app.route('/changeindex', methods=['POST'])
def reindex():
    if request.is_json:
        req = request.get_json()
        q = req['theme']
        m = int(req['maxtweets'])
        print(m)
        change_index_theme(q,m)
    return json.dumps({'success':True}), 200, {'ContentType':'application/json'} 

@app.route('/search', methods=['POST'])
def search():
    if request.is_json:
        req = request.get_json()
        q = req['search']
        k = int(req['k'])
        do_query(q,k)
        
    return json.dumps({'success':True}), 200, {'ContentType':'application/json'} 

if __name__ == "__main__":
    ii = InvertedIndex()
    ii.BSB_index_construction()
    app.secret_key = ".."
    app.run(port=8080, threaded=True, host=('127.0.0.1'))