# s3backup
```
pip3 install boto3==1.17.1

cd /opt/files
wget -o backup.py https://github.com/toolkt/s3backup/main/backup.py
python3 /opt/files/backup.py -c <S3_OBJECT_FOLDER> -d <DATABASE_NAME> -u <POSTGRES_USERNAME> -p <POSTGRES_PASSWORD> -a <S3_KEY> -s <S3_SECRET> -b <S3_BUCKET> -e <PATH>

example:
python3 /opt/files/backup.py -c client_folder -d client_database -u pguser -p pgpass -a ASDQWEALKSDFOI123SDA -s asdf98asd98asqekh13asd98hk123j98asdfh123 -b client-backups-bucket -e ap-south-1.linodeobjects.com

```