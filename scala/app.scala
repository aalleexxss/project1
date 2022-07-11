package com.Revature

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import java.sql.{Connection,DriverManager}

import scala.io.StdIn.readLine
import scala.util.control.Breaks._

class userData(var user_id: Int, var user_name: String, var pswd: String, var admin: Boolean) {
  def this() = {
    this(0, "", "", false)
  }
}

object JDBC1 {

  def main(args: Array[String]) {
    // create a spark session
    // for Windows
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("created spark session")

    val url = "jdbc:mysql://localhost:3306/climbing_cleaned"
    val user = "Alex"
    val pass = "Revature-P0"

    val areasDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","areas").option("user",user)
      .option("password",pass).load()

    val routesDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","routes").option("user",user)
      .option("password",pass).load()

    val brandsDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","brands").option("user",user)
      .option("password",pass).load()

    val gearDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","gear").option("user",user)
      .option("password",pass).load()

    val reviewsDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","reviews").option("user",user)
      .option("password",pass).load()

    val postsDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","posts").option("user",user)
      .option("password",pass).load()

    val commentsDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","comments").option("user",user)
      .option("password",pass).load()

    val usersDf = spark.read.format("jdbc").option("url",url)
      .option("dbtable","users").option("user",user)
      .option("password",pass).load()
    usersDf.createOrReplaceTempView("users")

    import spark.implicits._

    var useInfo = new userData()

    println("Hello! ")
    println("Would you like to log in (l) or sign up (s)?")
    print("-> ")

    // getting user data or sign up
    var flag: Boolean = true
    while(flag) {
      val LorS = readLine()
      if (LorS != "l" && LorS != "s") {
        println("I'm sorry, command not recognized")

      // option is l or s
      } else {
        // user wants to sign up
        if (LorS == "s") {
          print("enter user name: ")
          val user_name = readLine()
          print("enter password: ")
          val pswd = readLine()

          val query: String = s"SELECT * FROM users WHERE user_name = \"$user_name\""
          val userCheck = spark.sql(query)
          //checks if user already exists
          if (userCheck.count() == 0) {
            // inserts new user data, returns info to useInfo
            val userDf = Seq((user_name, pswd, false)).toDF("user_name", "pswd", "admin_flag")
            userDf.write.mode(SaveMode.Append).format("jdbc").option("url", url)
              .option("dbtable", "users").option("user", user)
              .option("password",pass).save()
            val userInfo = spark.sql(s"SELECT * FROM users WHERE user_name = \"$user_name\"")
            useInfo.user_id = userInfo.head().getInt(0)
            useInfo.user_name = userInfo.head().getString(1)
            useInfo.pswd = userInfo.head().getString(2)
            useInfo.admin = userInfo.head().getBoolean(3)
            flag = false
          } else {
            println("I'm sorry, that user name has already been taken")
          }
        // if user chooses to sign in
        } else {
          print("enter user name: ")
          val user_name = readLine()
          print("enter password: ")
          val pswd = readLine()
          val userInfo = spark.sql(s"SELECT * FROM users WHERE user_name = \"$user_name\" AND pswd = \"$pswd\"")
          if (userInfo.count() == 0) {
            println(s"incorrect password for $user_name, or user doesn't exist")
          } else {
            useInfo.user_id = userInfo.head().getInt(0)
            useInfo.user_name = userInfo.head().getString(1)
            useInfo.pswd = userInfo.head().getString(2)
            useInfo.admin = userInfo.head().getBoolean(3)
            flag = false
          }
        }

      }
      if (flag == true) {
        println("log in or sign up ")
        print("-> ")
      }
    }

    var main_flag = true
    while(main_flag) {
      println(" ----------------------------------------- ")
      println("| select (a) to see areas                 |")
      println("| select (r) to see routes                |")
      println("| select (b) to see brands                |")
      println("| select (p) to see products              |")
      println("| select (re) to see reviews              |")
      println("| select (po) to see posts                |")
      println("| select (c) to see comments              |")
      if (useInfo.admin) {
        println("| select (aq) to ask analytical questions |")
        println("| select (u) to see user data             |")
      }
      println("| select (q) to quit                      |")
      println(" ----------------------------------------- ")
      print("-> ")
      var choice = readLine()

      if (choice == "a") {
        println("How many areas would you like to see?: ")
        val num = readLine().toInt
        areasDf.show(num, false)
      } else if (choice == "r") {
        print ("How many routes would you like to see?: ")
        val num = readLine().toInt
        routesDf.createOrReplaceTempView("routes")
        areasDf.createOrReplaceTempView("areas")
        val routesView = spark.sql("SELECT routes.route_id, areas.area_location, routes.stars, routes.route_type, routes.rating, routes.rating, routes.pitches, routes.length, routes.votes FROM routes JOIN areas ON routes.location_id=areas.area_id").show(num, false)
      } else if (choice == "b") {
        print("How many brands would you like to see?: ")
        val num = readLine().toInt
        brandsDf.show(num)
      } else if (choice == "p") {
        brandsDf.createOrReplaceTempView("brands")
        gearDf.createOrReplaceTempView("gear")
        print("How many products would you like to see?: ")
        val num = readLine().toInt
        val productView = spark.sql("SELECT brands.brand_name, gear.model FROM brands JOIN gear ON brands.brand_id = gear.brand_id").show(num, false)
      } else if (choice == "re") {
        brandsDf.createOrReplaceTempView("brands")
        gearDf.createOrReplaceTempView("gear")
        reviewsDf.createOrReplaceTempView("reviews")
        print("How many reviews would you like to see?: ")
        val num = readLine().toInt
        val reviewView = spark.sql("SELECT brands.brand_name, gear.model, reviews.rating, reviews.review FROM brands JOIN gear ON brands.brand_id = gear.brand_id JOIN reviews ON reviews.gear_id = gear.gear_id").show(num, false)
      } else if (choice == "po") {
        println("How many posts would you like to see?: ")
        val num = readLine().toInt
        postsDf.show(num, false)
      } else if (choice == "c") {
        print("How many comments would you like to see?: ")
        val num = readLine().toInt
        postsDf.createOrReplaceTempView("posts")
        commentsDf.createOrReplaceTempView("comments")
        val commentsView = spark.sql("SELECT posts.topic, comments.comment_text, comments.likes FROM posts JOIN comments ON posts.post_id = comments.post_id").show(num, false)
      } else if (choice == "aq") {
        if (useInfo.admin){
          println("Questions: ")
          println(" --------------------------------------------------------------------- ")
          println("| 1. Do trad or sport routes have higher average likes                 |")
          println("| 2. What brand has the highest average likes                          |")
          println("| 3. What's the most \"controversial\" topic                             |")
          println("| 4. Do international or American routes have higher average stars     |")
          println("| 5. Do routes north or south of the equator have higher average stars |")
          println("| 6. What is the average number of likes for each route grade          |")
          println(" --------------------------------------------------------------------- ")
          val admin_choice = readLine()

          if (admin_choice == "1") {
            val sportDf = routesDf.filter($"route_type" contains "sport")
            val tradDf = routesDf.filter($"route_type" contains "trad")
            sportDf.createOrReplaceTempView("sport")
            tradDf.createOrReplaceTempView("trad")
            println("Sport routes:")
            val sportView = spark.sql("SELECT AVG(stars) FROM sport").show()
            println("Trad routes:")
            val tradView = spark.sql("SELECT AVG(stars) FROM trad").show()

          } else if (admin_choice == "2") {
            brandsDf.createOrReplaceTempView("brands")
            gearDf.createOrReplaceTempView("gear")
            reviewsDf.createOrReplaceTempView("reviews")
            val reviewView = spark.sql("SELECT brands.brand_name, gear.model, reviews.rating, reviews.review FROM brands JOIN gear ON brands.brand_id = gear.brand_id JOIN reviews ON reviews.gear_id = gear.gear_id")
            reviewView.createOrReplaceTempView("concat_review")
            val brandAvg = spark.sql("SELECT AVG(rating) AS avg_rating, brand_name FROM concat_review GROUP BY brand_name ORDER BY avg_rating DESC").show(75)

          } else if (admin_choice == "3") {
            postsDf.createOrReplaceTempView("posts")
            commentsDf.createOrReplaceTempView("comments")
            val commentsCount = spark.sql("SELECT COUNT(comment_id) AS comment_count, post_id FROM comments GROUP BY post_id")
            commentsCount.createOrReplaceTempView("commentCount")
            val scoreView = spark.sql("SELECT posts.topic, posts.likes, commentCount.comment_count, (posts.likes/commentCount.comment_count) AS score FROM posts JOIN commentCount ON posts.post_id = commentCount.post_id WHERE posts.likes != 0 ORDER BY score").show(500, false)

          } else if (admin_choice == "4") {
            areasDf.createOrReplaceTempView("areas")
            routesDf.createOrReplaceTempView("routes")
            val fullRouteDf = spark.sql("SELECT * FROM areas JOIN routes ON areas.area_id = routes.location_id")
            fullRouteDf.createOrReplaceTempView("joinedRoutes")
            val international = fullRouteDf.filter($"area_location" contains "international")
            val american = spark.sql("SELECT * FROM joinedRoutes WHERE area_location NOT LIKE \'%international\'")
            international.createOrReplaceTempView("inter")
            american.createOrReplaceTempView("amer")
            println("Average stars of american routes:")
            val amerAvg = spark.sql("SELECT AVG(stars) AS Average_stars FROM amer").show()
            println("Average stars of international routes:")
            val interAvg = spark.sql("SELECT AVG(stars) AS Average_stars FROM inter").show()

          } else if (admin_choice == "5") {
            routesDf.createOrReplaceTempView("routes")
            areasDf.createOrReplaceTempView("areas")
            val joinedDf = spark.sql("SELECT areas.latitude, routes.stars FROM areas JOIN routes ON routes.location_id = areas.area_id")
            val above = joinedDf.filter($"latitude" > 0)
            above.createOrReplaceTempView("sAbove")
            val below = joinedDf.filter($"latitude" < 0)
            below.createOrReplaceTempView("sBelow")
            println("Average stars above the equator: ")
            val result1 = spark.sql("SELECT AVG(stars) AS average_stars FROM sAbove").show()
            println("Average stars below the equator:")
            val result2 = spark.sql("SELECT AVG(stars) AS average_stars FROM sBelow").show()

          } else if (admin_choice == "6") {
            routesDf.createOrReplaceTempView("routes")
            val ratedGrades = spark.sql("SELECT rating, AVG(stars) AS average_stars FROM routes GROUP BY rating ORDER BY average_stars DESC").show(10000)

          } else {
            println("command not recognized, please try again")
          }


        } else {
          println("command not recognized, please try again")
        }
      } else if (choice == "u") {
        if (useInfo.admin) {
          usersDf.show()
        } else {
          println("command not recognized, please try again")
        }
      } else if (choice == "ar") {
        if (useInfo.admin) {
          print("TODO")
        }  else {
          println("command not recognized, please try again")
        }
      } else if (choice == "ap") {
        if (!useInfo.admin){
          println("TODO")
        } else {
          println("command not recognized, please try again")
        }
      } else if (choice == "q") {
        println("Goodbye!")
        main_flag = false
      } else {
        println("command not recognized, please try again")
      }

    }
  }
}