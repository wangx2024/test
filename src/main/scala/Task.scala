
import org.apache.spark.sql.functions.{col, first, lit}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
case class Peer(peer_id: String, id_1: String, id_2: String, year: Integer)



object Task {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Task").master("local").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val sizeGiven = if(args.length>0) args(0).toInt else 3

    val Data = Seq(
      Peer("ABC17969(AB)", "1", "ABC17969", 2022),
      Peer("ABC17969(AB)","2","CDC52533",2022),
      Peer("ABC17969(AB)","3","DEC59161",2023),
      Peer("ABC17969(AB)","4","F43874",2022),
      Peer("ABC17969(AB)","5","MY06154",2021),
      Peer("ABC17969(AB)","6","MY4387",2022),
      Peer("AE686(AE)","7","AE686",2023),
      Peer("AE686(AE)","8","BH2740",2021),
      Peer("AE686(AE)","9","EG999",2021),
      Peer("E686(AE)","10","AE0908",2021),
      Peer("AE686(AE)","11","QA402",2022),
      Peer("AE686(AE","12","OM691",2022)

    ).toDF()
    Data.show()
    Data.printSchema()
    //问题1
    //For each peer_id, get the year when peer_id
    //contains id_2, for example for ‘ABC17969(AB)’ year is 2022.

    val df1 = answerQ1(spark, Data).cache()
    println("问题1")
    df1.show()
    df1.printSchema()


    //问题2
    //Given a size number, for example 3. For each
    //peer_id count the number of each year
    //(which is smaller or equal than the year in step1).

    val df2 = answerQ2(spark, Data, df1).cache()
    println("问题2")
    df2.show()
    df2.printSchema()




      //

    val df3 = df2.orderBy("peer_id", "year")
      .groupBy("peer_id")
      .agg(functions.sum("count").alias("total_count"), first("year").alias("first_year"))
      .withColumn("is_valid", col("total_count") >= lit(3))
      .filter($"is_valid")
      .select("peer_id", "first_year")

    df3.show()





    spark.stop()


  }


  def answerQ1(ss: SparkSession, source: DataFrame): DataFrame = {
    import ss.implicits._
    source.filter($"peer_id".contains($"id_2"))
  }


  def answerQ2(ss: SparkSession, source: DataFrame, df1: DataFrame): DataFrame = {
    import ss.implicits._
    val df_tmp = source.groupBy($"peer_id",$"year").count()
    df_tmp.as("a")
      .join(df1.as("b"), $"a.peer_id" === $"b.peer_id","left_outer")
      .filter($"a.year" <= $"b.year")
      .select($"a.*")
  }



}
