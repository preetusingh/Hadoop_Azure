##Objective: 
####To find the highest and average score of a class for packed data using Hive User Defined Aggregating Function(UDAF) and System Defined Function(SDF) on Microsoft Azure Platform

The format of the record is
 <class_id><student_id><score>

Note: class_id: 3 digits, student_id: 5 digits, score: 3 digits

Used these two files to test the program
##### File 1

00112345093

00212325094

00312315088

00112355100

##### File 2

00111345087

00212225097

00222325084

00312315088

00112355100

00312316078

##Prerequisites
Download the below mentioned jars and configure into project build path

hadoop-common-2.7.0.jar

hadoop-core-1.1.2.jar

commons.logging-1.1.1.jar

hadoop-hdfs-2.7.1.jar

hive-exec-0.13.0.jar

JRE System Library[JavaSE-1.7] or above


##Project creation and execution steps

Write HiveMaxNAvg program in Eclipse and export it into a jar named as HiveMaxNAvg.jar.

###Create Cluster in Microsft Azure

login Azure account and Type HDInsight Cluster in search option.

Click Add button, Provide cluster name and fill up all the information to create the cluster like Cluster type: Interactive Hive, Operating System: Linux

Click credentials to expand and set the username and password for both cluster and ssh.

Click Data Source to expand and create a new storage [if you want you can also use existing storage] and container. Set the location to “WEST US”

Configure the pricing and keep it default. you can change the number of working node as per  need too.

Create a new resource group PracticeHive. Check the pin to dashboard and create. It will take around 20 -30 minutes to create the cluster.

###SSH to Azure cluster

Copy the Hostname to login from windows laptop using putty and ssh command to login from another linux

Open  putty and use hostname to SSH to  azure cluster

Enter credentials that was provided earlier while creating cluster

####User Defined Aggregating Function Script:
Create a file named as HiveMaxNAvgUDAF.hql and copy the below mentioned script into it

add jar /home/admin123/hive_folder/HiveMaxNAvg.jar;
CREATE TEMPORARY FUNCTION maximum AS
'HiveMaxNAvg.Maximum';
CREATE TEMPORARY FUNCTION mean AS
'HiveMaxNAvg.Mean';
CREATE EXTERNAL TABLE IF NOT EXISTS
stud_record_udf_local (
classid int, studentid int, score int
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.RegexSerDe' WITH
SERDEPROPERTIES (
"input.regex" = "(\\d{3})(\\d{5})(\\d{3})"
);
LOAD DATA LOCAL INPATH
'/home/admin123/hive_folder/File*'
overwrite into table stud_record_udf_local;
SELECT classid,maximum(score),mean(score) FROM
stud_record_udf_local GROUP BY classid;

####System Defined Function Script:
Create a file named as HiveMaxNAvgSDF.hql and copy the below mentioned script into it

DROP TABLE IF EXISTS stud_record_sdf_local;
CREATE TABLE stud_record_sdf_local (classid int,
studentid int, score int)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.RegexSerDe' WITH
SERDEPROPERTIES (
"input.regex" = "(\\d{3})(\\d{5})(\\d{3})"
);
LOAD DATA LOCAL INPATH
'/home/admin123/hive_folder/File*'
OVERWRITE INTO TABLE stud_record_sdf_local;
SELECT classid, MAX(score), AVG(score) FROM
stud_record_sdf_local GROUP BY classid;

###Upload the jar file to Azure Cluster WinSCP download link
https://winscp.net/eng/download.php#download2

Open the WinSCP use the cluster hostnameto SFTP using 22 port. Use to SSH username/password

Drag and drop the jar file and input file (File1.txt and File2.txt) from your local system to hive_folder

Open putty. Go to your hive_folder location and verify whether all files are created successfully in same location.

Execute the Hive script for System Defined Function:

$ hive -f MaxMeanSDF.hql > MaxMeanSDF.txt

Execute the Hive script for User Defined Function:

$ hive -f MaxMeanUDF.hql > MaxMeanUDF.txt

##Result

View output: 

$ cat MaxMeanSDF.txt

$ cat MaxMeanUDF.txt
