# ATHENA QUERY LAUNCHER

##  INSTALLATION 
Install required packages available in requirements.txt with pip3/pip.
pip install -r requirements.txt

## PREPARATION
### Any platform
Download the access key, secret key, secret token with the privilege to run your queries and put them
in the conf/aws_data.ini file.
Inside the conf.ini file you can specify the following parameters.
- launch_date: the date that will substitute the NOW() function inside all the queries

### Windows
In the file conf/file_to_launch_list.txt insert in each row the .sql file of the you want to execute.

### Ubuntu
Work in progress.

## Execution
Run the main.py file using the interpreter.
python main.py

## Reulsts 
For each executed query a json result will be available in the folder results.