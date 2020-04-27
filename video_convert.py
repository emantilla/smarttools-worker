import boto3
import subprocess
from pymongo import MongoClient
import datetime
import smtplib
import logging
import json
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import os

from botocore.exceptions import ClientError
from apscheduler.schedulers.blocking import BlockingScheduler

sched = BlockingScheduler()


@sched.scheduled_job('interval', minutes=1)
def timed_job():
    proccess_video()


def get_connection_sqs():
    sqs = boto3.client('sqs', region_name= os.environ.get('region_name'), aws_access_key_id=os.environ.get('aws_access_key_id'),
                       aws_secret_access_key=os.environ.get('aws_secret_access_key'))
    return sqs


def get_connection_s3():
    s3 = boto3.client('s3', region_name=os.environ.get('region_name'), aws_access_key_id=os.environ.get('aws_access_key_id'),
                      aws_secret_access_key=os.environ.get('aws_secret_access_key'))
    return s3


def get_connection_bd():
    client = MongoClient(os.environ.get('host'))
    db_con = client.smarttoolsdb
    return db_con


def get_unproccessed_video():
    # Create SQS client
    sqs = get_connection_sqs()
    queue_url = os.environ.get('queue_url')

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=300,
        WaitTimeSeconds=0
    )

    try:
        messages = response['Messages']
        videos = messages[0]
        return videos
    except KeyError:
        print('No hay mensajes')
        return None


def proccess_video():
    print('Inicio proccess_video')
    videos = get_unproccessed_video()
    if videos is not None:
        print('Encontro mensajes en la cola')
        receipt_handle = videos['ReceiptHandle']
        video = json.loads(videos['Body'])
        path = video['video_file']
        file_name = str(path).split('/')[1]
        get_video(file_name, path)
        output = file_name.split('.')
        output = output[0]
        now = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_")
        new_file_name = 'convert_' + now + output + '.mp4'
        print(datetime.datetime.now().strftime("%Y %m %d %H:%M:%S ") +
                     'converting video: {}'.format(file_name))
        subprocess.run(" ffmpeg -i {} -vcodec h264 -acodec aac {}".format(file_name, new_file_name), shell=True)

        if upload_file(new_file_name):
            delete_message(receipt_handle)
            subprocess.run("rm {}".format(file_name), shell=True)
            subprocess.run("rm {}".format(new_file_name), shell=True)
            update_video_status_converted(video['id'], new_file_name)
            send_email(video['id'])
        print(datetime.datetime.now().strftime("%Y %m %d %H:%M:%S  ") +
                     'video converted: {}'.format(file_name))


def get_video(file_name, object_name):
    media = str(os.environ.get('converted_bucket')).split('/')
    object_name = media[0] + '/' + object_name
    s3 = get_connection_s3()
    print ('file_name' + file_name + '  object_name'+ object_name)
    try:
        s3.download_file(os.environ.get('bucket_name'), object_name, file_name)
    except ClientError as e:
        print(e)


def delete_message(receipt_handle):
    sqs = get_connection_sqs()
    queue_url = os.environ.get('queue_url')
    sqs.delete_message(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle
    )


def upload_file(file_name):
    object_name = os.environ.get('converted_bucket') + file_name

    # Upload the file
    s3_client = get_connection_s3()
    bucket = os.environ.get('bucket_name')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name, ExtraArgs={'ACL':'public-read'})
    except ClientError as e:
        logging.error(e)
        return False
    return True


def update_video_status_converted(_id, file_name):
    object_name = os.environ.get('converted_bucket') + file_name

    mydb = get_connection_bd()
    mycol = mydb['concursos_uservideo']
    myquery = {'id': _id}
    newvalues = {"$set": {'video_converted': object_name, 'convert_state': 1}}

    mycol.update_one(myquery, newvalues)


def send_email(_id):
    em = get_uservideo(_id)
    concurso = get_concurso(em['concurso_id'])
    body = """<body>
    <p>Hola %s queremos agradecerte por participar en el concurso %s. Ingresa al <a href="%s/%s">sitio web</a> del concurso para ver tu video.</p>
    <br>
    <p>Cordialmente el equio de SmartTools</p>
    </body>""" % (em['user_name'], concurso['name'], os.environ.get('base_url'), concurso['uniq_url'])
    message = Mail(
    from_email='smarttools-api@example.com',
    to_emails=em['user_email'],
    subject="{} - Revisa tu participación en el concurso {}".format(str(em['user_name']), str(concurso['name'])),
    html_content=body)
    try:
        sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
        response = sg.send(message)
        update_video_status_sended(_id)
        print(response.status_code)
        print(response.body)
    except Exception as e:
        print(e.message)
    
    
def config_email(filename='db_conf.ini', section='ses_aws'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    email = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            email[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))

    return email


def get_uservideo(_id):

    mydb = get_connection_bd()
    mycol = mydb['concursos_uservideo']
    myquery = {'id': _id}

    uservideo = mycol.find_one(myquery)
    return uservideo


def get_concurso(_id):

    mydb = get_connection_bd()
    mycol = mydb['concursos_concurso']
    myquery = {'id': _id}

    concurso = mycol.find_one(myquery)
    return concurso

def update_video_status_sended(_id):

    mydb = get_connection_bd()
    mycol = mydb['concursos_uservideo']
    myquery = {'id': _id}
    newvalues = {"$set": {'email_send': 1}}

    mycol.update_one(myquery, newvalues)


sched.start()
