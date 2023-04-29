from datetime import datetime, timedelta
import mysql.connector
import json
from flask import make_response, jsonify
from config.config import dbconfig


class user_model():
    def __init__(self):
        self.con = mysql.connector.connect(host=dbconfig['host'],user=dbconfig['username'],password=dbconfig['password'],database=dbconfig['database'])
        self.con.autocommit=True
        self.cur = self.con.cursor(dictionary=True)
        
    def all_user_model(self):
        self.cur.execute("SELECT * FROM users")
        result = self.cur.fetchall()
        if len(result)>0:
            return {"payload":result}

        else:
            return "No Data Found"
    def get_specific_user(self,userid):
        user = (userid,)
        self.cur.execute("SELECT * FROM users where name=%s",user)
        result = self.cur.fetchall()
        if len(result)>0:
            return {"payload":result}

        else:
            return "No Data Found"
        
    def get_user_search(self,name, location):
        user = (name,location,)
        self.cur.execute("SELECT * FROM users where name=%s and location=%s",user)
        result = self.cur.fetchall()
        if len(result)>0:
            return {"payload":result}

        else:
            return "No Data Found"
        

        
