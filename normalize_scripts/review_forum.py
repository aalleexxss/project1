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

# gets data from climbing.review_forum, inserrts into climbing_cleaned.posts
sql = "SELECT topic, review_text, join_date, post_date, likes FROM review_forum WHERE page_num=1 AND post_num=0;"
sess.cursor.execute(sql)
data = sess.cursor.fetchall()

for i in data:
    sql = "INSERT INTO posts (topic, post, join_date, post_date, likes) VALUES (%s, %s, %s, %s, %s)"
    vals = [i[0], i[1], i[2], i[3], i[4]]
    sess2.cursor.execute(sql, vals)
    sess2.connection.commit()    

# gets data from climbing.review_forum, inserts into climbing_cleaned.comments
# comments.post_id is matched to proper posts.post_id
sql = "SELECT topic, review_text, join_date, post_date, likes FROM review_forum WHERE post_num!=0;"
sess.cursor.execute(sql)
data = sess.cursor.fetchall()

for i in data:
    
    sql2 = "SELECT post_id FROM posts WHERE topic=%s"
    val = [i[0]]
    sess2.cursor.execute(sql2, val)
    data2 = sess2.cursor.fetchall()
    if len(data2) > 0:
        sql3 = "INSERT INTO comments (post_id, comment_text, join_date, post_date, likes) VALUES (%s, %s, %s, %s, %s)"
        vals = [data2[0][0], i[1], i[2], i[3], i[4]]
        sess2.cursor.execute(sql3, vals)
        sess2.connection.commit()
