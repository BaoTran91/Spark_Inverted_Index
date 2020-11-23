import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object InvertedIndex {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder().appName("Inverted Index").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    //register udf to get document index from filename
    val getFileName = spark.udf.register("get_only_file_name", (fullPath: String) => fullPath.split("/").last)

    //PATH of the dataset
    val path = "dataset/*"

    // READ all files from the dataset folder into value column and add the filename column (document id)
    val txtdf = spark.read.text(path).withColumn("filename", getFileName(input_file_name))

    //Explode lines into words separated by space
    val worddf = txtdf.withColumn("value", explode(split($"value", " ")))

    // Register the DataFrame as a SQL temporary view
    worddf.createOrReplaceTempView("words_df")


//    Create a data frame with the word and document id. Run some quick data cleansing.
    val file_df = spark.sql("""
      SELECT DISTINCT REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(TRIM(value)),'.',''),',',''),'"',''),'!',''),'?',''),':',''),';',''),'(',''),')',''),'/',''),"'",''),'*',''),'[',''),"]",'') AS words,
      CAST(filename as int) document
       FROM words_df
       WHERE LOWER(TRIM(value)) REGEXP "[a-zA-Z]+.*" -- Have at least 1 letters
       AND value NOT REGEXP "[0-9]+.*" -- No Numbers
       AND value NOT LIKE '%**%'
       AND value NOT LIKE '%--%'
       AND trim(value) NOT LIKE '-%'
       AND value NOT LIKE '%\_%'
       AND value NOT LIKE '%|%'
       AND value NOT LIKE '%>%'
    """)

    file_df.createOrReplaceTempView("file_df")

    // Get distinct words to create dictionary with word id
    val dict = spark.sql("""
      SELECT DISTINCT
      words
      FROM file_df
    """)

    //    Create word id from incrementing by word order.
    val window = Window.orderBy(dict.col("words"))
    val dictionary = dict.withColumn("word_id", row_number().over(window))

    //write dictionary to a file. CSV is chosen but it's easier to view.
    dictionary
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter","|")
      .save("dictionary")

    dictionary.createOrReplaceTempView("dict")

    //join word id and doc id into a complete DataFrame
    val agg_df = spark.sql("""
      SELECT DISTINCT
      -- d.words,
      f.document,
      d.word_id
      FROM file_df f
      JOIN dict d
      ON f.words = d.words
      ORDER BY word_id,document
    """)
    agg_df.show()

    // Concatenated ARRAY TO WRITE AS CSV (Flat file.)
    // Can use array with other file type like parquet and ORC which are more optimal.
    // Using CSV cause it's easier to view.
    val final_df = agg_df
      .groupBy("word_id")
      .agg( concat_ws(",", array_sort(collect_list("document"))).as("doc_id"))
//      .agg( array_sort(collect_list("document")).as("document")) // keeping as list.
      .orderBy("word_id")

    final_df.show()

    final_df
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter","|")
      .save("inverted_index")
  }
}