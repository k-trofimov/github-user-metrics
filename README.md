Github Archive Analser

This app is not finished

The idea is to provision the infrastructure via deployment of a helm chart to Kubernetes cluster
It will create the following resources:
 - Airflow as a scheduler
 - Spark cluster as runtime environment
 - Postgres as a DB backend (later replaces with ClickHouse)
 - Grafana as a Dashboard

At the moment only the spark job and airflow scheduler are in the finished state
Security is insured via 

    
