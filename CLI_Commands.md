Start Airflow

``` start_airflow ```

Source dataset : [https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz]

Create a directory structure for staging area as follows
``` /home/project/airflow/dags/finalassignment/staging ```

Navigate to 

``` cd airflow/dags ```

Create directories

```
sudo mkdir finalassignment

cd finalassignment

sudo mkdir staging

cd staging

```

Download sourcefile

``` sudo wget [https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz] ```

May need to change permission settings

``` sudo chmod 777 /home/project/airflow/dags/finalassignment ```

After unziping the downloaded (note this command is running in the python dag)

Do the following to clean the tsv file
```
cut -f1-7 tollplaza-data.tsv|tr -d "\r" >clean_tollplaza-data.csv
```
