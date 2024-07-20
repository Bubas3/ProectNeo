import os
with open('.env', 'r') as file:
    lines = file.readlines()
    for line in lines:
        key, value = line.strip().split('=')
        os.environ[key] = value

DBNAME = os.environ['dbname']
DBUSER = os.environ['dbuser']
DBPASS = os.environ['dbpass']
DBHOST = os.environ['dbhost']
DBPORT = os.environ['dbport']
