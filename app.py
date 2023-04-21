from flask import Flask

app = Flask(__name__)

@app.route("/")
def hellWorld():
    return "hello world"

@app.route("/home")
def Home():
    return "This is home page"

@app.route("/myRoute")
def My():
    return "This is my page"    

try:
    from controllers import *
except Exception as e:
    print(e)