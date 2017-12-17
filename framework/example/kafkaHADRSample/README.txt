
The test is to check the trapezium workflow for a HADR simulation

Setup required:
---------------
- create HADR_2 topic with 4 partitions
- Build the project and check the target/ directory
- create a file /opt/bda/environment     (Keep 'nonlocal' as the content in that file)

Test Run:
---------------
- Unless defaults are changed for Kafka and Zookeeper, no need to edit the conf files
- run the workflow from the scripts/start.sh in one window/process
- run the same workflow in another window/process
- check the output of the dataframe
- kill one of the spark context in one window/process
- check how topic switches to another spark context and processed

Test Run of producer:
---------------------
- To simulate messages, Run Producer.java Check the host and port of the zookeeper and run.