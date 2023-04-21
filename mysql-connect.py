import mysql.connector
cnx = mysql.connector.connect(user='root', password='password', host='localhost', database='dbms_project', port=3306)
cursor = cnx.cursor()
cursor.execute("SELECT * FROM twitter_user")
results = cursor.fetchall()
for row in results:
    print(row)
cursor.close()
cnx.close()
print("bhushna")