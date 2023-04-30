# Why Dockerfile here?

We need to packing Python virtual environment in order to run Spark job in EMR Serverless

As mention in [EMR Serverless guideline](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-python-libraries.html):

> You must run the following commands in a similar Amazon Linux 2 environment with the same version of Python as you use in EMR Serverless, that is, Python 3.7.10 for Amazon EMR
> release 6.6.0. You can find an example Dockerfile in the EMR Serverless Samples GitHub repository.

[Problem](https://stackoverflow.com/a/73060861/14774515) if you dont packaging Python venv in Amazon Linux 2, you may waste a day just to submit job, follow me; [Solution](https://stackoverflow.com/a/74323431/14774515)

## Structure

```sh
scripts
├── Dockerfile -> to Packaging the Python virtual environment in Amazon Linux 2 OS (MUST)
├── pyspark_venv.tar.gz -> that Packaging environment we will send to s3
├── random_text_classification.py -> Script we run
├── requirements.txt
└── README.md
```

## Step

- make sure you are in this `scripts` folder
- `DOCKER_BUILDKIT=1 docker build --output . . -f dags/scripts/Dockerfile-packing`
- after execute that command, a file name `pyspark_venv.tar.gz` should be under `scripts` folder
- `chmod 755 dags/scripts/pyspark_venv.tar.gz`
