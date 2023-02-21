package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, lit}

object IcebergPlayground {

  def run(): Unit = {
    setupIceberg()
    setupParquet()

    copy1()
    //copy2()
  }

  /* Initialize an iceberg table */
  def setupIceberg(): Unit = {

    val spark = SparkSession
      .builder
      .appName("myapp")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg.type", "hadoop")
      .config("spark.sql.catalog.iceberg.warehouse", "./warehouse")
      .getOrCreate

    spark.sql("""
        CREATE TABLE iceberg.db.events (
          event_id string,
          context_1 int,
          context_2 int,
          load_tstamp timestamp
        )
        USING ICEBERG
        """)

    spark.sql("""
      INSERT INTO iceberg.db.events (event_id, context_1, context_2, load_tstamp)
      VALUES ('aaa', 42, 42, current_timestamp())
      """)

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

  /** Uses Spark SQL to copy new transformed events into iceberg table
   *
   *  **Problem 1**: In the copy statement, we MUST specify every column of the target table, or
   *  else we get an exception. i.e. must specify context_2 even though the source files do not
   *  contain context_2. This might be a problem with multiple parallel streaming apps; could there
   *  be a race condition if another instance adds a context column?
   */
  def copy1(): Unit = {

    val spark = SparkSession
      .builder
      .appName("myapp")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg.type", "hadoop")
      .config("spark.sql.catalog.iceberg.warehouse", "./warehouse")
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

    // Prepare iceberg table to receive the new context
    spark.sql("""
        ALTER TABLE iceberg.db.events
        ADD COLUMN context_new int
        """)

    // And now copy
    spark.sql("""
      INSERT INTO iceberg.db.events (event_id, context_1, context_2, context_new, load_tstamp)
      SELECT event_id, context_1, null, context_new, current_timestamp() FROM parquet_source
      """)

    spark.close()
  }

  /** Uses Spark's dataframe APIs to copy new transformed events into iceberg table
   *
   *  Note this does not solve the problem described in `copy1`: we still need to specify context_2 in the dataframe.
   */
  def copy2(): Unit = {

    val spark = SparkSession
      .builder
      .appName("myapp")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "./warehouse")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg.type", "hadoop")
      .config("spark.sql.catalog.iceberg.warehouse", "./warehouse")
      .getOrCreate

    // Prepare iceberg table to receive the new context
    spark.sql("""
        ALTER TABLE iceberg.db.events
        ADD COLUMN context_new int
        """)

    spark
      .read
      .parquet("./warehouse/test_parquet")
      .withColumn("load_tstamp", current_timestamp())
      .withColumn("context_2", lit(null))
      .writeTo("iceberg.db.events")
      .append()

    spark.close()
  }

}
