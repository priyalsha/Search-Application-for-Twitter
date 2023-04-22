import mysql.connector
import json
class user_model():
    def __init__(self):
        try:
            self.con = mysql.connector.connect(user='root', password='password', host='localhost', database='dbms_project', port=3306)
            print("Some success")
            self.cur = self.con.cursor(dictionary=True)
        except:
            print("Some error")
    def search_user(self):
        self.cur.execute("SELECT * FROM twitter_user")
        results = self.cur.fetchall()
        for row in results:
            print(row)
        if len(results)>0:
            return json.dumps(results)
        else:
            return "No Data Found"