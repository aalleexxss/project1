import mysql.connector

# checks for duplicate brand names
def stop_dups(lst, brand):
    for x in lst:
        if brand == x[1]:
            return False
    return True

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

# takes data from climbing.trailspace, inserts into makes climbing_cleaned.brands
sql = "SELECT DISTINCT brand FROM trailspace LIMIT 0, 2000;"
sess.cursor.execute(sql)
brandData = sess.cursor.fetchall()
for i in brandData:

    sql2 = "INSERT INTO brands (brand_name) VALUES (%s);"
    val = [i[0]]
    sess2.cursor.execute(sql2, val)
    sess2.connection.commit()

# takes data from climbing.trailspace, inserts into climbing_cleaned.brands.
# climbing_cleand.brands is used to get proper brand_id matched with gear.
sql = "SELECT * FROM temp_gear;"
sess2.cursor.execute(sql)
data = sess2.cursor.fetchall()

dup_list = [[data[0][1], data[0][2]]]
print(str(data[0]))

print(dup_list)

for i in data:
    if (stop_dups(dup_list, i[2])):
        dup_list.append([i[1], i[2]])

sql2 = "INSERT INTO gear (brand_id, model) VALUES (%s, %s);"

for j in dup_list:
    sess2.cursor.execute(sql2, j)
    sess2.connection.commit()

# takes data from climbing.trailspace, inserts into climbing_cleaned.reviews
# gear_id is matched to proper review
sql = "SELECT model, rating, review_text FROM trailspace;"
sess.cursor.execute(sql)
data = sess.cursor.fetchall()

count = 0
for i in data:
    count += 1
    sql2 = "SELECT gear_id, model FROM gear WHERE model=%s"
    val = [i[0]]
    sess2.cursor.execute(sql2, val)
    data2 = sess2.cursor.fetchall()
    
    sql3 = "INSERT INTO reviews (gear_id, rating, review) VALUES (%s, %s, %s)"

    for i2 in data2:
        vals = [int(i2[0]), float(i[1]), i[2]]
        sess2.cursor.execute(sql3, vals)
        sess2.connection.commit()