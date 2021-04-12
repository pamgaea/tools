# S3 Multipart Uploader

This is a script for uploading large files to S3

To run:

```bash
./upload --file=relative_or_full_path --bucket=the_s3_bucket_name --key=the_s3_object_key --region=the_region_of_the_s3_bucket --content-type=the_content_type_of_the_file [--upload-id=the_upload_id_of_the_multipart_upload]
```
