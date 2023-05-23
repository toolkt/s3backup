# -*- coding: utf-8 -*-
import os, time
import sys, getopt
import subprocess
from subprocess import PIPE,Popen
import shlex
from optparse import OptionParser
from datetime import datetime, timedelta

import boto3


DB_USER = ''
DB_PASS = ''
DB_NAME = []
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_BUCKET_NAME = ''
ENDPOINT = ''
TK_CLIENT = ''

def main(argv):


    # python3  pg2s3.py -c local -d test1,pino -u odoomgr -p odoomgr -a 3EEDXX -s 5lXXXXXXXXX -b odoo-client-backups -e ap-south-1.linodeobjects.com
    backup_path = '/opt/backups'
    if not os.path.exists(backup_path):
        os.makedirs(backup_path)


    try:
        opts, args = getopt.getopt(argv,"hc:d:u:p:a:s:b:e:",["client=","db=","user=","pass","access=","secret=","bucket=","endpoint="])
    except getopt.GetoptError:
        print("Error")
        print("pg2s3.py -c <client> -d <dbname1,dbname2> -u <dbuser> -a <AWS_ACCESS_KEY_ID> -s <AWS_SECRET_ACCESS_KEY> -b <AWS_BUCKET_NAME> -e <ENDPOINT>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("pg2s3.py -c <client> -d <dbname1,dbname2> -u <dbuser> -a <AWS_ACCESS_KEY_ID> -s <AWS_SECRET_ACCESS_KEY> -b <AWS_BUCKET_NAME> -e <ENDPOINT>")
            sys.exit()


        elif opt in ("-c", "--client"):
            TK_CLIENT = arg
        elif opt in ("-d", "--db"):
            DB_NAME = arg.split(',')
        elif opt in ("-u", "--user"):
            DB_PASS = arg
        elif opt in ("-p", "--pass"):
            DB_USER = arg
        elif opt in ("-a", "--key"):
            AWS_ACCESS_KEY_ID = arg
        elif opt in ("-s", "--secret"):
            AWS_SECRET_ACCESS_KEY = arg
        elif opt in ("-b", "--bucket"):
            AWS_BUCKET_NAME = arg
        elif opt in ("-e", "--path"):
            ENDPOINT = arg


    linode_obj_config = {
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
        "endpoint_url": 'https://%s' % ENDPOINT,
    }
    client = boto3.client("s3", **linode_obj_config)

    # now = datetime.now() - timedelta(days=26)
    now = datetime.now()

    for db in DB_NAME:
        filename_daily = "%s.daily.%s" % (db,now.strftime('%Y%m%d'))
        filename_weekly = "%s.weekly.%s" % (db,now.strftime('%Y%b%V').upper())
        filename_monthly = "%s.monthly.%s" % (db,now.strftime('%Y%b').upper())

        # print(filename_daily,filename_weekly,filename_monthly)

        destination = r'%s/%s' % (backup_path, filename_daily)
        s3_destination = '%s/%s' % (TK_CLIENT,filename_daily)
        s3_weekly_destination = '%s/%s' % (TK_CLIENT,filename_weekly)
        s3_monthly_destination = '%s/%s' % (TK_CLIENT,filename_monthly)

        # Backup Postgres DB
        backup_postgres_db('127.0.0.1', db, '5432', DB_USER, DB_PASS, destination, False)


        #Upload to S3
        client.upload_file(Filename=destination, Bucket=AWS_BUCKET_NAME, Key=s3_destination)

        copy_source = {
            'Bucket': AWS_BUCKET_NAME,
            'Key': s3_destination
        }

        client.copy(copy_source, AWS_BUCKET_NAME, s3_weekly_destination)
        client.copy(copy_source, AWS_BUCKET_NAME, s3_monthly_destination)



        # Delete old files from S3
        response = client.list_objects(Bucket=AWS_BUCKET_NAME, Prefix=TK_CLIENT+'/'+db)
        if 'Contents' in response:
            for object in response['Contents']:

                #check if object is daily backup
                if 'daily' in object['Key']:
                    obj_date = datetime.strptime(object['Key'][-8:], '%Y%m%d')
                    days_ago = now - timedelta(days=7)
                    if obj_date < days_ago:
                        client.delete_object(Bucket=AWS_BUCKET_NAME, Key=object['Key'])

                #check if object is weekly
                if 'weekly' in object['Key']:
                    week = object['Key'][-9:]
                    file_week = int(week[:4]+''+week[-2:])
                    current_week = int(now.strftime('%Y%V'))
                    if file_week < (current_week-4):
                        client.delete_object(Bucket=AWS_BUCKET_NAME, Key=object['Key'])

        # Delete files older than 5 days from local
        for f in os.listdir(backup_path):
            timenow = time.time()
            if os.stat(os.path.join(backup_path,f)).st_mtime < timenow - 5 * 86400:#86400
                os.remove(os.path.join(backup_path, f))

def week_number_of_month(date_value):
     return (date_value.isocalendar()[1] - date_value.replace(day=1).isocalendar()[1] + 1)

def backup_postgres_db(host, database_name, port, user, password, dest_file, verbose):
    """
    Backup postgres db to a file.
    """
    if verbose:
        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-Fc',
                 '-f', dest_file,
                 '-v'],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)
    else:

        try:
            process = subprocess.Popen(
                ['pg_dump',
                 '--dbname=postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, database_name),
                 '-Fc',
                 '-f', dest_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if process.returncode != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)




def upload_to_s3(file_full_path, dest_file, AWS_BUCKET_NAME, linode_obj_config):

    client = boto3.client("s3", **linode_obj_config)
    client.upload_file(Filename=file_full_path, Bucket=AWS_BUCKET_NAME, Key=dest_file)

def delete_from_s3(filename, AWS_BUCKET_NAME, linode_obj_config):

    client = boto3.client("s3", **linode_obj_config)
    client.delete_object(Bucket=AWS_BUCKET_NAME, Key=filename)



if __name__ == "__main__":
    main(sys.argv[1:])