import imp
import flask
from flask import request
from app import app
from model.user_model import user_model
obj = user_model()

@app.route("/user/all")
def all_users():
    return obj.all_user_model()


@app.route("/user/<name>")
def specific_user(name):
    return obj.get_specific_user(name)


@app.route("/search")
def get_search_user():
    arg1 = request.args['arg1']
    arg2 = request.args['arg2']
    return obj.get_user_search(arg1,arg2)
