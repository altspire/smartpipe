from flask import Flask
from flask import render_template
from flask.ext.pymongo import PyMongo

app = Flask(__name__)
mongo = PyMongo(app)

# connect to another MongoDB database on the same host
app.config['MONGO2_DBNAME'] = 'canon'
mongo2 = PyMongo(app, config_prefix='MONGO2')

@app.route('/')
def home_page():
    patients = mongo2.db.patients.find()
    return render_template('hello.html', patients=patients)