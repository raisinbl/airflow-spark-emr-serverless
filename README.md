<!---------------------->
# What is this

# Prequisted

- Docker
  - Read carefully [how to run Airflow in docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- AWS Connection
  - You should configure the AWS credentals first by setting IAM Credential for User in IAM services, In Access management, add neccessary permissions to user and roles.
  - AWS CLI
  
# How to run

> There're `requirements.txt` if you want to testing

- Get data
  
  ```sh
  mkdir ./dags/data
  wget https://www.dropbox.com/sh/amdyc6z8744hrl5/AACZ6P5QnM5nbX4Gnk9_JW0Ma/movie_review/movie_review.csv?dl=0
  mv movie_review* ./dags/data/movie_review.csv
  ```

- Setting up credential aws credentials and config under `~/.aws` or you can check it and create it by `aws configure` and the credential file is `~/.aws/credentials`
  - `aws configure export-credentials` to get key and password if you wish manually setting up connection.
  <!-- - esle: `cp ~/.aws . -R` -->
  ```sh
  docker build -t apache/airflow:2.5.3 .
  docker compose up -d
  ```

- Packing PySpark environment
  
  ```sh
  cd dags/scripts
  DOCKER_BUILDKIT=1 docker build --output . . -f Dockerfile
  chmod 755 dags/scripts/pyspark_venv.tar.gz
  ```

- Testing task in Airflow:
  - In local(case you don't want to use docker):
    link the project dags dir to airflow dir :`ln -s dags/ ~/airflow/` <br>
    `airflow tasks test <dags-id> <task-id>` <br>
    e.x: `airflow tasks test 1-emr-serverless create_bucket`
  - In docker:
    `./airflow.sh tasks test 1-emr-serverless create_bucket`
