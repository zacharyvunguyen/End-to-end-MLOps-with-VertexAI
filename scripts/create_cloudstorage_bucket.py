from google.cloud import storage

def create_bucket(base_bucket_name, project='babyweight-mlops', location='us-central1'):
    """
    Creates a new bucket in Google Cloud Storage based on the base name.

    Parameters:
    base_bucket_name (str): The base name of the bucket to create.
    project (str, optional): The ID of the GCP project in which to create the bucket. Defaults to 'babyweight-mlops'.
    location (str, optional): The location in which to create the bucket. Defaults to 'us-central1'.

    Returns:
    Bucket: The created bucket.
    """
    bucket_name = f"{base_bucket_name}"

    storage_client = storage.Client(project=project)
    bucket = storage_client.bucket(bucket_name)
    new_bucket = storage_client.create_bucket(bucket, location=location)

    print(f"Bucket {new_bucket.name} created in {new_bucket.location} with storage class {new_bucket.storage_class}")
    return new_bucket
