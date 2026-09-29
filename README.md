# ETL-on-Patient-dataframe-using-Airflow
My project is an ETL pipeline implemented using Apache Airflow. The purpose is to process patient data and identify the total number of patients who have blood group A+ and whose status is either Under Treatment or Recovered.
The pipeline consists of five stages: Extract, Validate, Transform, Load and Notification.”
“In the Extract stage, I read the patient CSV dataset using Pandas and save the extracted data temporarily for the next stage.”
“In the Validate stage, I perform validation on four important columns: Blood_Type, Status, Age and Last_Visit_Date. I also check for duplicate records.”
“In the Transform stage, I filter the data based on the required business condition: Blood_Type should be A+, and Status should be either Under Treatment or Recovered. I then calculate the total number of matching patients.”
“I also perform a group-by aggregation on Status to see the count for each status.”
“In the Load stage, I save the transformed patient records as a CSV file and generate a summary text file containing the validation columns, transformation condition and final patient count.”
“Finally, the Notification task confirms that the complete ETL pipeline has finished successfully.”
“Airflow manages the execution order, so Extract runs first, followed by Validate, Transform, Load and Notification.
