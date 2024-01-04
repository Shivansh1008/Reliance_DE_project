import traceback
from src.main.utility.logging_config import *



def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix,file_name):
        #response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)
    try:
        for obj in s3_client.Bucket(bucket_name).objects.filter(Prefix=source_prefix):
            source_key = obj.key
            if source_key.endswith(file_name):
                source_object=s3_client.Object(bucket_name,source_key)

                destination_key = destination_prefix + source_key[len(source_prefix):]
                destination_object=s3_client.Object(bucket_name,destination_key)

                destination_object.copy_from(CopySource={'Bucket':bucket_name ,'Key':source_key})
                obje = s3_client.Object(bucket_name, source_key)
                obje.delete()
            #s3_client.delete_object(Bucket=bucket_name, Key=source_key)
        return f"Data Moved succesfully from {source_prefix} to {destination_prefix}"

    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e

def moves_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix):
    try:
        objects_to_delete = []
        for obj in s3_client.Bucket(bucket_name).objects.filter(Prefix=source_prefix):
            source_key = obj.key
            
            # Construct the destination key based on the source key and destination prefix
            destination_key = destination_prefix + source_key[len(source_prefix):]
            destination_object = s3_client.Object(bucket_name, destination_key)

            # Copy the object from source to destination
            destination_object.copy_from(CopySource={'Bucket': bucket_name, 'Key': source_key})

            objects_to_delete.append({'Key': source_key})
            # Delete the source objects after successful copy
        if objects_to_delete:
                objects_to_delete = [obj for obj in objects_to_delete if not obj['Key'].endswith('/')]
                s3_client.Bucket(bucket_name).delete_objects(Delete={'Objects': objects_to_delete})
        return f"Data Moved succesfully from {source_prefix} to {destination_prefix}"

    except Exception as e:
        logger.error(f"Error moving file : {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e

def move_local_to_local():
    pass
