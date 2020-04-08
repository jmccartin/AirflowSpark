
# AirflowSpark    <img src="https://airflow.apache.org/_images/pin_large.png" width="42"> <img src="https://spark.apache.org/images/spark-logo-trademark.png" width="80">
An example project for productionising workflows via integration between Apache Airflow and Spark in a cloud-based environment.

<img src="https://github.com/jmccartin/AirflowSpark/workflows/build%20status/badge.svg">

## Introduction
AirflowSpark is a software project for the productionisation of data science projects via testable code. 
It uses Airflow as a batch scheduler, with a Spark computation layer designed to be provisioned on a cloud-based 
elastic compute platform (such as AWS/EMR). The project automatically creates a Airflow's Directed Acyclic Graphs (DAGs) for each workflow, 
which may encompass a mix of ETL steps and data science tasks. Airflow Operators are run sequentially inside each DAG, 
each of which is a separate Python class, which defines the work to be done. The Airflow server hosts these DAGs, 
and schedules the execution time based upon each workflow's configuration.

#### Project structure
Rather than following a layout typical to a Python application, support for multiple languages has been envisioned.
The majority and core of the program will still be Python, but small bits of Scala or Java (both of which have APIs to Spark) could be included for performance reasons.
The source code for the project can be found in `src/main`, with the any tests found in a duplicate folder structure under `src/test`.
The code for each workflow belongs in a named directory under `src/main/python/projects`. The name of the directory is automatically used as the DAG name in Airflow.

## Airflow Server Setup
This area is still a WIP.
### AWS
On an empty EC2 instance, create a new linux user called Airflow with passwordless sudo rights.
```bash
sudo su -
adduser -G wheel airflow
sed 's/# %wheel\tALL=(ALL)\tNOPASSWD: ALL/%wheel\tALL=\(ALL\)\tNOPASSWD: ALL/' /etc/sudoers
```
create a log folder for airflow
```bash
mkdir /var/log/airflow
chown airflow /var/log/airflow
```
create a persistent varable for AIRFLOW_HOME across all users env
```bash
echo export AIRFLOW_HOME=/home/airflow/.airflow > /etc/profile.d/airflow.sh
```
Switch to the airflow user, and copy the authorized keys file to the airflow users home.
```bash
su - airflow
mkdir .ssh
touch .ssh/authorized_keys
chmod 700 .ssh
chmod 644 .ssh/authorized_keys
```
Set the necessary environment variables
```bash
echo ". /opt/anaconda3/etc/profile.d/conda.sh" >> ~/.bashrc
```
If Anaconda is not installed, install it to the path above.
Create a new python environment for the project from within the project root folder
(you may use conda/venv or anything that suits):
```bash
python -m venv .venv
```

#### Creating Postgres Backend
Set up a Postgres RDS server using AWS (web interface). Install `psql` on the Airflow instance if not already done so. Create a user on the 
```bash
psql \
  --host=<hostname>
  --port=5432
  --username=postgres
  --password
```
Once in the postgres shell, create the airflow user (use an alphanumeric password only):
```bash
postgres=# CREATE USER airflow PASSWORD ‘<password>’;
CREATE ROLE
postgres=# CREATE DATABASE airflow;
CREATE DATABASE
postgres=# GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT
```

#### Create the project area
As the airflow user, create a directory and then deploy the code.
```bash
mkdir -p ~/Github/AirflowSpark
```
Deploy the code to the airflow instance using the shell script from your local machine.
```bash
sh scripts/deploy_code.sh . airflow /home/airflow/Github/AirflowSpark
```
On the airflow ec2 instance, install the airflow requirements to the AirflowSpark python environment:
```bash
python -m pip install -r requirements.txt
```
Export the AirflowSpark directory to the `PYTHONPATH` sys var:
```bash
export PYTHONPATH=/home/airflow/Github/AirflowSpark/
```
Initialise the airflow database once, edit the configuration, and then reinitialise:
```bash
export AIRFLOW_HOME=/home/airflow/.airflow
airflow initdb
vi ~/.airflow/airflow.cfg
airflow initdb
```
Finally, start the server and scheduler:
```bash
airflow webserver -p <PORT>
airflow scheduler
```
### Local
While it should be possible to run Airflow locally repeating many of the instructions above, this has not been tested in some time.

## Todo list
* Integrate other cloud envrionments (Azure, GCP) via Airflow helpers and python scripts equivalent to `boto3`.
* Better logging:
  * currently Airflow just polls the Spark master for the status of the job and not the stdout/stderr.
  * Spark status logs are duplicated ~10 times per poll
  * A custom logger should be used for each python log call, to integrate better with Airflow.

