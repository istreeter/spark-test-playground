package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.current_timestamp

object DeltaPlayground {

  def run(): Unit = {
    setupDelta()
    setupParquet()

    // copy1()
    copy2()
  }

  /* Initialize a delta table */
  def setupDelta(): Unit = {

    val spark = SparkSession
      .builder
      .appName("myapp")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./warehouse")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate

    spark.sql("""
        CREATE TABLE test_delta (
          event_id string,
          context_1 int,
          context_2 int,
          load_tstamp timestamp
        )
        USING DELTA
        """)

    spark.sql("""
      INSERT INTO test_delta (event_id, context_1, context_2, load_tstamp)
      VALUES ('aaa', 42, 42, '2022-02-02 00:00:00')
      """)

    spark.sql("OPTIMIZE test_delta")

    spark.close()
  }


  /* Create a directory of plain parquet files to use as input.
   *
   * This simulates the output of a transformation step run elsewhere.  It adds a new context that is not in the delta table.
   */
  def setupParquet(): Unit = {

    val spark = SparkSession
      .builder
      .appName("myapp")
      .master("local[1]")
      .config("spark.sql.warehouse.dir", "./warehouse")
      .getOrCreate

    spark.sql("""
        CREATE TABLE test_parquet (
          event_id string,
          context_1 int,
          context_new int
        )
        USING parquet
        """)

    spark.sql("""
      INSERT INTO test_parquet (event_id, context_1, context_new)
      VALUES ('aaa', 42, 42), ('bbb', 42, 42), ('ccc', 42, 42)
      """)

    spark.close()

  }

  /** Uses Spark SQL to copy new transformed events into delta table
   *
   *  **Problem 1**: In the copy statement, we MUST specify every column of the target table, or
   *  else we get an exception. i.e. must specify context_2 even though the source files do not
   *  contain context_2. This might be a problem with multiple parallel streaming apps; could there
   *  be a race condition if another instance adds a context column?
   *
   *  **Problem 2**: Because of a bug, this won't work with generated columns like `DAY(load_tstamp)`
   *  https://github.com/delta-io/delta/issues/1215
   */
  def copy1(): Unit = {

    val spark = SparkSession
      .builder
      .appName("myapp")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./warehouse")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate

    // Tell Spark where to find the files
    spark.sql("""
        CREATE EXTERNAL TABLE parquet_source (
          event_id string,
          context_1 int,
          context_new int
        )
        USING parquet
        LOCATION './test_parquet'
        """)

    // Tell Spark where to find the delta table
    spark.sql("""
        CREATE TABLE test_delta
        USING delta
        LOCATION './test_delta'
        """)

    // Prepare delta table to receive the new context
    spark.sql("""
        ALTER TABLE test_delta
        ADD COLUMN context_new int
        """)

    // And now copy
    spark.sql("""
      INSERT INTO test_delta (event_id, context_1, context_2, context_new, load_tstamp)
      SELECT event_id, context_1, null, context_new, current_timestamp() FROM parquet_source
      """)

    spark.close()
  }

  /** Uses Spark's dataframe APIs to copy new transformed events into delta table
   *
   *  This seems to solve both the problems described in `copy1`
   */
  def copy2(): Unit = {

    val spark = SparkSession
      .builder
      .appName("myapp")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./warehouse")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate

    spark
      .read
      .parquet("./warehouse/test_parquet")
      .withColumn("load_timestamp", current_timestamp())
      .write
      .format("delta")
      .mode("append")
      .option("mergeSchema", true)
      .save("./warehouse/test_delta")

    spark.close()
  }

}
