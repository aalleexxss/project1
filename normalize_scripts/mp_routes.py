import mysql.connector

class SessionData:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host = "127.0.0.1",
            user = "Alex",
            password = "pswd",
            database = "climbing"
        )
        self.cursor = self.connection.cursor()

class Session2Data:
    def __init__(self):
        self.connection = mysql.connector.connect(
            host = "127.0.0.1",
            user = "root",
            password = "pswd",
            database = "climbing_cleaned"
        )
        self.cursor = self.connection.cursor()

sess = SessionData()
sess2 = Session2Data()

# Gets location data from climbing.mp_routes, 
# puts location, lat, and long into climbing_cleaned.areas
query = "SELECT DISTINCT location, latitude, longitude FROM mp_routes LIMIT 0, 30000;"
sess.cursor.execute(query)
data = sess.cursor.fetchall()
for i in data:
    
    print("count: " + str(count))
    print("area: " + i[0])
    print("lat: " + str(i[1]))
    print("long: " + str(i[2]))
    print("--------------------------")
    

    sql = "INSERT INTO areas (area_location, latitude, longitude) VALUES (%s, %s, %s);"
    vals = (i[0], i[1], i[2])
    sess2.cursor.execute(sql, vals)
    sess2.connection.commit()

# Gets data from climbing.mp_routes,
# puts data into mp_routes. uses area_location from climbing_cleaned.areas
# to put proper location_id in climbing_cleaned.routes
query = "SELECT * FROM mp_routes;"
sess.cursor.execute(query)
data = sess.cursor.fetchall()

for i in data:
    
    sql = "SELECT area_id, area_location FROM areas WHERE area_location=%s LIMIT 0, 200000"
    val = [i[2]]
    sess2.cursor.execute(sql, val)
    data2 = sess2.cursor.fetchall()
    
    for i2 in data2: 
        sql2 = "INSERT INTO routes (route_name, location_id, stars, route_type, rating, pitches, length, route_description, votes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
        vals = (i[1], int(i2[0]), float(i[3]), i[4], i[5], int(i[6]), float(i[7]), i[10], int(i[11]))
        sess2.cursor.execute(sql2, vals)
        sess2.connection.commit()