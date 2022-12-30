import json
import psycopg2
from os import environ

endpoint=environ.get('ENDPOINT')
port=environ.get('PORT')
dbuser=environ.get('DBUSER')
password=environ.get('DBPASSWORD')
database=environ.get('DATABASE')

def lambda_handler(event, context):
    region     = event['Records'][0]['awsRegion']
    bucket     = event['Records'][0]['s3']['bucket']['name']
    key        = event['Records'][0]['s3']['object']['key']
    s3KeyParts    = key.split('_')

    s3fullPath = 's3://' + bucket + '/' + key

    # query = 'COPY public.' + s3KeyParts[1] + ' from ' + s3fullPath + '" iam_role "arn:aws:iam::657118880000:role/Redshift-s3-readonly-role-01" region "' + region + '" delimiter "," csv quote as \'"\';'
    query = 'SELECT COUNT(*) FROM public.' + s3KeyParts[1]
    print(query)

    conn_str="host={0} dbname={1} user={2} password={3} port={4}".format(
                endpoint, database, dbuser, password, port)
    conn = psycopg2.connect(conn_str)
    conn.autocommit=True
    print(conn)

    cursor = conn.cursor()

    print(cursor.execute(query))
    cursor.close()


