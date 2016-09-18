from flask import Flask
from flask import send_from_directory

app = Flask(__name__, static_url_path='/static')

@app.route('/')
def send_root():
    return app.send_static_file('index.html')

@app.route('/facetview2/<path:path>')
def send_facetview(path):
    return send_from_directory('static/facetview2', path)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
