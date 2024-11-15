# Provider Visits

Project Specifications
Programming Language: Scala
Framework: Apache Spark
Output Format: JSON files
Data Sources
Within the data folder, you’ll find two CSV files for input:

providers.csv - This file contains information about healthcare providers, including their specialties.
visits.csv - This file includes records of unique visits, detailing the visit ID, associated provider ID, and the date of service.


## Problems

Feel free to use this repository as a basis for solving these problems. You can also supply your own code.

1. Given the two data datasets, calculate the total number of visits per provider. The resulting set should contain the provider's ID, name, specialty, along with the number of visits. Output the report in json, partitioned by the provider's specialty. 

2. Given the two datasets, calculate the total number of visits per provider per month. The resulting set should contain the provider's ID, the month, and total number of visits. Output the result set in json.

Please provide your code in some format so we can attempt to run it during evaluation. Examples include github/gitlab links, .scala files, zipped folders containing the modified source project.


Submission
To allow for project evaluation, please submit the code in any of the following formats:

A GitHub or GitLab repository link
.scala source files
A zipped folder containing the modified project source

Run Commands
sbt compile
sbt run