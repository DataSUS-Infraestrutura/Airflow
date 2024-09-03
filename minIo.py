from minio import Minio
from minio.error import S3Error

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    # client = Minio("localhost:9000", secure=False,
    #                access_key="DBmwcqOcEpW16SMKNR7i",
    #                secret_key="iYmk1FuK4RucU1u2mgrG63NZpT9B2AS5SbG7wajy",
    #                )

    client = Minio("play.min.io:9000",
                   access_key="XbXYqaZYnHyJt11oUUpH",
                   secret_key="vgOM9AZqwt9Oa2GPPVPJ4Eizn47HJT2JQN37sJQd",
                   )

    # The file to upload, change this path if needed
    source_file = "test-file.txt"

    # The destination bucket and filename on the MinIO server
    bucket_name = "primeiro"
    destination_file = "fabio-file.txt"

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    # Upload the file, renaming it in the process
    client.fput_object(
        bucket_name, destination_file, source_file,
    )
    print(
        source_file, "successfully uploaded as object",
        destination_file, "to bucket", bucket_name,
    )
