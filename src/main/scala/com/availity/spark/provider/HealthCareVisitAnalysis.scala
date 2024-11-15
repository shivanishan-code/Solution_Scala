import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object HealthCareVisitAnalysis {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val session = SparkSession.builder()
      .appName("HealthCareVisitAnalysis")
      .master("local[*]")
      .getOrCreate()

    import session.implicits._

    // Load provider information from providers.csv
    val providerInfo = session.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("data/providers.csv")

    // Load visit information from visits.csv
    val visitRecords = session.read
      .option("header", "false")
      .csv("data/visits.csv")
      .toDF("visitID", "providerID", "dateOfService")

    // Convert dateOfService to date type
    val visitsWithDate = visitRecords.withColumn("dateOfService", to_date($"dateOfService"))

    // Task 1: Compute the total number of visits per provider
    val providerVisitCounts = visitsWithDate
      .groupBy("providerID")
      .agg(count("visitID").alias("totalVisitCount"))

    // Join provider and visit data, create full name column
    val providerVisitDetails = providerInfo
      .join(providerVisitCounts, providerInfo("provider_id") === providerVisitCounts("providerID"))
      .select(
        providerInfo("provider_id"),
        concat_ws(" ", $"first_name", $"middle_name", $"last_name").alias("providerFullName"),
        $"provider_specialty",
        $"totalVisitCount"
      )

    // Write the results partitioned by provider specialty
    providerVisitDetails
      .repartition($"provider_specialty")
      .write
      .partitionBy("provider_specialty")
      .json("output/total_visits_per_provider")

    // Task 2: Calculate the monthly number of visits per provider
    val visitsWithMonth = visitsWithDate
      .withColumn("serviceMonth", date_format($"dateOfService", "yyyy-MM"))

    val monthlyVisitsByProvider = visitsWithMonth
      .groupBy("providerID", "serviceMonth")
      .agg(count("visitID").alias("monthlyVisitCount"))
      .orderBy("providerID", "serviceMonth")

    // Write the monthly visit data as JSON
    monthlyVisitsByProvider
      .write
      .json("output/monthly_visits_per_provider")

    // Stop SparkSession
    session.stop()
  }
}
