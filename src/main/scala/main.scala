import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Main {
    val spark = SparkSession.builder().master("local[1]").appName("testApp").getOrCreate()
    val data_path = "C:\\Users\\User\\Documents\\New folder (4)\\New folder (2)\\data\\"

    def main(args: Array[String]) {
        val raw_file_path = "Crime_Data_from_2020_to_Present.csv"
        // val raw_file_path = "/opt/spark-data/Crime_Data_from_2020_to_Present.csv"
        val raw = spark.read.option("header",true).format("csv").load(data_path + raw_file_path)
        
        val location_old_columns = List("AREA", "AREA NAME", "Rpt Dist No", "LOCATION", "Cross Street")
        val location_new_columns = List("area_id", "area_name", "reported_district", "location", "cross_street")
        val locations = raw
            .select(get_new_colums(location_old_columns, location_new_columns)
            .map(m=>m):_*).distinct().withColumn("id", monotonically_increasing_id)
            .select("id", "area_id", "area_name", "reported_district", "location", "cross_street")
        val location_count = raw.select(col("LOCATION").as("location")).groupBy("location").count()
        write_to_file(location_count, "location_count")
        
        
        val conv_date_time = udf((dte: String, tme: String) => dte.substring(0,10) + " "+ tme.substring(0,2) + ":" + tme.substring(2,4) + ":00")
        print("converted to date time")
        val date_time = raw
            .select(
                col("DR_NO"),
                to_timestamp(conv_date_time(col("Date Rptd"), col("TIME OCC")), "MM/dd/yyyy HH:mm:ss").as("reported_date_time")
            )
            .select(
                col("DR_NO").as("reported_no"),
                year(col("reported_date_time")).as("reported_year"),
                month(col("reported_date_time")).as("reported_month"),
                dayofmonth(col("reported_date_time"))as("reported_day"),
                hour(col("reported_date_time")).as("reported_hour"),
                minute(col("reported_date_time")).as("reported_minute")
            )

        write_to_file(date_time, "dim_reported_dates")

        val daily_count = date_time.groupBy("reported_year", "reported_month", "reported_day").count()
        val monthly_count = date_time.groupBy("reported_year", "reported_month").count()
        val yearly_count = date_time.groupBy("reported_year").count()

        write_to_file(daily_count, "daily_count")
        write_to_file(monthly_count, "monthly_count")
        write_to_file(yearly_count, "yearly_count")

    } 

    def write_to_file(df: DataFrame, file_path: String, base_dir: String = data_path): Unit = {
        df.write.option("header", true).format("delta").save(base_dir + file_path)
    }
    
    def get_new_colums(old_columns: List[String], new_columns: List[String]): List[Column] = {
        val columns = old_columns.zip(new_columns).map(f=>col(f._1).as(f._2))
        columns
    }
}