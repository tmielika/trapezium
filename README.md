# Build Trapezium
Trapezium is a maven project. Following instructions will create Trapezium jar for your repository.
1. git clone <git_url>
2. mvn clean install

# Adding a maven dependency
Once Trapezium jar is available in your maven repository, you can add dependency on Trapezium using following dependency elements to your pom.xml

          <dependency>
            <groupId>com.verizon.bda</groupId>
            <artifactId>trapezium-framework</artifactId>
            <version>1.0.0-SNAPSHOT</version>
          </dependency>

          <dependency>
            <groupId>com.verizon.bda</groupId>
            <artifactId>trapezium-framework</artifactId>
            <type>test-jar</type>
            <scope>test</scope>
            <version>1.0.0-SNAPSHOT</version>
          </dependency>

# Setting up Cluster environment variable
On all your Spark nodes, create a file /opt/bda/environment and add environment for your cluster, e.g., DEV|QA|UAT|PROD. You can do this through a setup script so that any new node to your cluster will have this file automatically created. This file allows Trapezium to read data from different data sources or data locations based on your environment. 

Let's take an example. Suppose you read data from HDFS in your application. In your DEV cluster, data resides in /bda/**dev**/data while in your PROD cluster data resides in /bda/**prod**/data. Now environment file indicates Trapezium where to read data from.

# Creating test applications with Trapezium
### Example #1
Create an application that reads data from hdfs://my/first/trapezium/app/input and persists output to hdfs://my/first/trapezium/app/output. Data must be read in batch mode every 15 min. Any new data that has arrived since the last successful batch run must be presented as DataFrame to the application code. No validations are needed on the data source.

### Solution
1. Set up environment file correctly on your cluster (See **Setting up Cluster environment variable**)
2. Trapezium needs 2 config files

	**< ENVIRONMENT >_app_mgr.conf**

		appName = "MyTestApplication"
		persistSchema = "app_mgrTest_Schema"
		tempDir = "/tmp/"
		sparkConf = {
			kryoRegistratorClass = "com.verizon.bda.trapezium.framework.utils.EmptyRegistrator"
			spark.akka.frameSize = 100
		}

		zookeeperList = "localhost:2181"
		applicationStartupClass = "com.verizon.bda.trapezium.framework.apps.TestManagerStartup"
		fileSystemPrefix = "hdfs://"

	**< WORKFLOW_NAME >.conf**

		runMode = "BATCH"
		dataSource = "HDFS"
		hdfsFileBatch = {
			batchTime = 900
			timerStartDelay = 1
			batchInfo = [
    			{
		      		name = "source1"
				dataDirectory = {
					local = "test/data/local"
					dev = "test/data/dev"
					prod = "test/data/prod"
				}
				validation = {
                        columns = ["col1"]
                        datatypes = ["String"]
                        minimumColumn = 1
                        dateFormat = "yyyy-MM-dd HH:mm:ss"
                        delimiter = ","
                        rules = {
                        }
                      }				
			} ]
		}
		transactions = [{
			transactionName = "my.first.trapezium.application.TestTransaction"
			inputData = [{
    				name = "source1"
			}]
			persistDataName = "myFirstOutput"
		}]

3. Implement my.first.trapezium.application.Transaction scala object

		package my.first.trapezium.application
		object TestTransaction extends BatchTransaction {
			val logger = LoggerFactory.getLogger(this.getClass)
			override def processBatch(df: Map[String, DataFrame], wfTime: Time): DataFrame = {
				logger.info("Inside process of TestTransaction")
				require(df.size > 0)
				val inData = df.head._2
				**Your TRANSFORMATION code goes here**
				inData
			}
			override def persistBatch(df: DataFrame, batchTime: Time): Unit = {
				df.write.parquet("my/first/Trapezium/output")
			}
		}

4. Submitting your Trapezium job

	spark-submit --master yarn --class com.verizon.bda.trapezium.framework.ApplicationManager --config <configDir> --workflow <workflowName>

	ApplicationManager is your driver class. configDir is the location of your environment configuration file created in step #2. workflowName is your workflow config file that you created in step #2. workflow config file must be present under config directory.

	If you want to submit your job in cluster mode, environment and workflow config files must be present inside your application jar.

	config is an optional command line parameter while workflow is a required one.
	
	
# Features
1. Workflow dependency 


    Any workflow can depend on another workflow to build a complex business pipeline.
       Config entry for defining workflow dependency
        dependentWorkflows={
            workflows=["workflow_1", "workflow_2"]
            frequencyToCheck=100
        }


2. Multiple source input
   Multiple source can define in workflow.
   
        inputData = [
        { 
          name = "source1"
        },
        { 
          name = "source2"
        }
        ]
3. Support Adhoc jobs, Developer run, QA run, integration run and long running jobs
       Workflow will run only one time for Adhoc,QA
       Config entry only time time run.
             oneTime = "true"
        Long running jobs
           oneTime = "true" 
       
4. Supported Data types
 Trapezium can read data in various formats including text, gzip, json, avro and parquet
    Config entry for reading fileFormat
    
            fileFormat="avro"
            fileFormat="json"
            fileFormat="parquet”

5. Reading modes
    Trapezium supports reading data in batch as well streaming mode
        Config entry for reading in batch mode
       
        runMode="STREAM"
        batchTime=5
    
    Config entry for reading in stream mode
         
         runMode="BATCH"
         batchTime=5
   
   Read data by timestamp
        offset=2
 
6. Data Validation
    Validates data at the source based on rule defined.
        Filters out all invalid rows and log.
        
        validation = 
            {  columns = ["name", "age", "birthday", "location"]
                datatypes = ["String", "Int", "Timestamp", "String"]
                dateFormat = "yyyy-MM-dd HH:mm:ss"
                delimiter = "|"
                minimumColumn = 4
                rules = {
                    name=[maxLength(30),minLength(1)]
                    age=[maxValue(100),minValue(1)]
                   }
          }    

# Frisky checks

Internal Verizon Developers are mandated to run frisky before creating any PR on Github Master / Verizon Stash to 
clean any sensitive information from the code and datasets

# Frisky Usage

git clone https://github.com/Verizon/frisky
git clone https://github.com/Verizon/trapezium
cd trapezium
python ../frisky/frisky .

# Example violation
 
./simulation/src/main/resources/local_app_mgr.conf:30 has non RFC-1918 IP address contents
Clean all such violations before creating PR

(c) Verizon

Contributions from:

* Pankaj Rastogi (@rastogipankaj)
* Debasish Das (@debasish83)
* Hutashan Chandrakar(@hutashan)
* Pramod Lakshmi Narasimha (@pramodl)
* Sumanth Venkatasubbaiah (@vsumanth10)
* Faraz Waseem
* Ken Tam
* Ponrama Jegan
* Venkatesh poosarla(@venkateshpoosarla)
* Narendra Parmar(@narendrasfo)
* Venkatesh Pooserla

And others (contact Pankaj Rastogi / Debasish Das if you've contributed code and aren't listed).
