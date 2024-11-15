import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions._

class HealthCareVisitAnalysisTest extends AnyFunSuite with BeforeAndAfterAll {

  // Initialize a Spark session
  val spark = SparkSession.builder()
    .appName("HealthCareVisitAnalysisTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  // Sample data for testing
  val providerData = Seq(
    ("P1", "John", "A", "Doe", "Cardiology"),
    ("P2", "Jane", "B", "Smith", "Dermatology")
  ).toDF("provider_id", "first_name", "middle_name", "last_name", "provider_specialty")

  val visitData = Seq(
    ("V1", "P1", "2023-01-01"),
    ("V2", "P1", "2023-01-15"),
    ("V3", "P2", "2023-02-10"),
    ("V4", "P1", "2023-01-30")
  ).toDF("visitID", "providerID", "dateOfService")

  override def afterAll(): Unit = {
    spark.stop()
  }

  test("Task 1: Count total visits per provider") {
    val visitsWithDate = visitData.withColumn("dateOfService", to_date($"dateOfService"))
    val providerVisitCounts = visitsWithDate
      .groupBy("providerID")
      .agg(count("visitID").alias("totalVisitCount"))

    val expectedVisitCounts = Seq(
      ("P1", 3),
      ("P2", 1)
    ).toDF("providerID", "totalVisitCount")

    assertDataFrameEquality(providerVisitCounts, expectedVisitCounts)
  }

  test("Task 2: Generate full name and join provider visit counts") {
    val visitsWithDate = visitData.withColumn("dateOfService", to_date($"dateOfService"))
    val providerVisitCounts = visitsWithDate
      .groupBy("providerID")
      .agg(count("visitID").alias("totalVisitCount"))

    val providerVisitDetails = providerData
      .join(providerVisitCounts, providerData("provider_id") === providerVisitCounts("providerID"))
      .select(
        providerData("provider_id"),
        concat_ws(" ", $"first_name", $"middle_name", $"last_name").alias("providerFullName"),
        $"provider_specialty",
        $"totalVisitCount"
      )

    val expectedDetails = Seq(
      ("P1", "John A Doe", "Cardiology", 3),
      ("P2", "Jane B Smith", "Dermatology", 1)
    ).toDF("provider_id", "providerFullName", "provider_specialty", "totalVisitCount")

    assertDataFrameEquality(providerVisitDetails, expectedDetails)
  }

  test("Task 3: Calculate monthly visits per provider") {
    val visitsWithDate = visitData.withColumn("dateOfService", to_date($"dateOfService"))
    val visitsWithMonth = visitsWithDate.withColumn("serviceMonth", date_format($"dateOfService", "yyyy-MM"))

    val monthlyVisitsByProvider = visitsWithMonth
      .groupBy("providerID", "serviceMonth")
      .agg(count("visitID").alias("monthlyVisitCount"))
      .orderBy("providerID", "serviceMonth")

    val expectedMonthlyCounts = Seq(
      ("P1", "2023-01", 3),
      ("P2", "2023-02", 1)
    ).toDF("providerID", "serviceMonth", "monthlyVisitCount")

    assertDataFrameEquality(monthlyVisitsByProvider, expectedMonthlyCounts)
  }

  def assertDataFrameEquality(actualDf: DataFrame, expectedDf: DataFrame): Unit = {
    assert(actualDf.collect().toSet == expectedDf.collect().toSet)
  }
}
