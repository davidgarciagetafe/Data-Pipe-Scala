package com.evobanco.test

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

object IsbnEncoder {
  implicit def dmcEncoder(df: DataFrame) = new IsbnEncoderImplicit(df)
}

class IsbnEncoderImplicit(df: DataFrame) extends Serializable {

  /**
    * Creates a new row for each element of the ISBN code
    *
    * @return a data frame with new rows for each element of the ISBN code
    */
  def explodeIsbn(): DataFrame = {
    val auxdf =  df.
      withColumn("aux",col("isbn")).
      withColumn("aux",translate(col("aux"),"-","")).
      withColumn("aux",regexp_extract(col("aux"),"\\d{13}",0))

   val notISBNdf= auxdf.
      where(length(col("aux")) =!= 13).
     drop("aux")


    val yesISBNdf= auxdf.
      where(length(col("aux")) === 13).
      withColumn("a2",concat(lit("ISBN-EAN: "),col("aux").substr(0,3))).
      withColumn("a3",concat(lit("ISBN-GROUP: "),col("aux").substr(4,2))).
      withColumn("a4",concat(lit("ISBN-PUBLISHER: "),col("aux").substr(6,4))).
      withColumn("a5",concat(lit("ISBN-TITLE: "),col("aux").substr(10,3))).
      select(col("name"),col("year"),expr("stack(5,'isbn',isbn,'a2',a2,'a3',a3,'a4',a4,'a5',a5) as(B,isbn)")).
      drop("B")

    yesISBNdf.union(notISBNdf)
  }

}
