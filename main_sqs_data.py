from bson import ObjectId
from datetime import datetime
import certifi
import boto3
import os
import traceback
from dotenv import load_dotenv
load_dotenv()
from pymongo import MongoClient
import requests
import time
from io import BytesIO

def main_func_data(pdf_list_data):
    try:
        destination_bucket = 'lawbucketfiles'
        destination_folder = 'South Africa/Central/laws/'
        bucket_name = destination_bucket
        s3_file_name = destination_folder
        try:
            s3 = boto3.client('s3')            
            s3 = boto3.client(
                's3',
                aws_access_key_id='AKIAXOCIQ5CGTFMOS3E4',
                aws_secret_access_key='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',
                region_name='us-west-1'
            )

        except NoCredentialsError:
            print("Error: AWS credentials not available.")

        headers2 = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br, zstd',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Cookie': '_ga=GA1.2.1812274488.1728036163; _gid=GA1.2.2146775531.1728036163; _ga_JGJB9CHQCE=GS1.2.1728049728.2.1.1728051048.32.0.1272949412',
            'Host': 'www.saflii.org',
            'If-Modified-Since': 'Tue, 17 Sep 2024 10:48:40 GMT',
            'If-None-Match': '"12ad-6224e6ebdd386-gzip"',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36'
        }
        pdf_url_list = pdf_list_data
        session = requests.Session()
        count = 1
        for pdf_ur in range(len(pdf_url_list)):
            pdf_url = pdf_url_list[pdf_ur]
            response  = session.get(pdf_url['url'], headers=headers2)
            time.sleep(2)
            if response.status_code == 200:
                pdf_file_in_memory = BytesIO(response.content)
                time.sleep(2)
                new_filename = pdf_url['name'].strip()
                new_filename = new_filename.replace('/' , ' ')
                s3_file_data =  s3_file_name+ new_filename+'.pdf'
                s3.upload_fileobj(pdf_file_in_memory, bucket_name , s3_file_data)
                print('DataCheck : ' , s3_file_data)
                print(f"Successfully uploaded to S3: {s3_file_data}")
                print("Count :::: ",count)

                print('*******************')
                print(new_filename+'.pdf')
                print('****************')
                df = {
                    "_id":ObjectId(),
                    "name": new_filename.lower().strip(),
                    "type": 'law',
                    "description":'' ,
                    "document_name":new_filename.strip(),
                    "language":ObjectId('655b04b3ee48964061eb7392') ,
                    "doc": new_filename+'.pdf',
                    "doc_path":destination_folder,
                    "dontUpdateAutomatically": False,
                    "country":[ObjectId("67077d71533511f6fb462989")] ,
                    "institution":None , 
                    "central": ObjectId('6708bfec1868e72c30e8c578'),
                    "state": [],
                    "active": False,
                    "vectorized": False,
                    "createdAt": datetime.today(),
                    "updatedAt": datetime.today()
                }
                def send_meta_data(document , rendering):
                    client = MongoClient("mongodb+srv://linkinlegal:F9P3wfhJvRMaWz4c@linkinlegal.5as5thv.mongodb.net/linkinlegal?retryWrites=true&w=majority" , tlsCAFile=certifi.where())
                    db = client["linkinlegal"]
                    collection = db[str(rendering)]
                    existing_object = collection.find_one({'name': document['name']})

                    if existing_object:
                        print(f"Object >>>>>>>>> '{document['name']}' already exists in the collection." , document['doc_path'] ,'  :: ',  existing_object)
                    else:
                        insert_result = collection.insert_one(document)
                        print(f"Inserted document ID ------> : {insert_result.inserted_id}" , "doc: " , existing_object)
                        print('successfully sent to mongo : ' ,insert_result.inserted_id)
                send_meta_data(document=df , rendering='lawsandregulatorymodels')
                SQS_AWS_ACCESS_KEY_ID = 'AKIAXOCIQ5CGTFMOS3E4'
                SQS_AWS_SECRET_ACCESS_KEY = '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'
                AWS_REGION = 'ap-south-1'
                sqs_queue_url = 'https://sqs.ap-south-1.amazonaws.com/511253211277/linkinlegal-DB'
                sqs = boto3.client('sqs', aws_access_key_id=SQS_AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=SQS_AWS_SECRET_ACCESS_KEY,
                                    region_name=AWS_REGION)

                def create_send_vectorize_tasks_sqs(file_id):
                    try:
                        response = sqs.send_message(
                            QueueUrl=sqs_queue_url,
                            MessageAttributes={
                                'file_id': {
                                    'DataType': 'String',
                                    'StringValue': str(file_id)
                                } 
                            },
                            MessageBody='Vectorizing main laws, regulations and agreements!',
                        )
                        print("Successfully sent message to sqs")
                        return True
                    except Exception as e:
                        print(f"Failed to send message to SQS: {e}")
                        return False
                create_send_vectorize_tasks_sqs(file_id=df['_id'])

                print('***************************************')
                count+=1

            else:
                print(response.status_code)

    except:
        traceback.print_exc()



        







