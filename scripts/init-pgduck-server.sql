-- create localstack aws secret
CREATE SECRET s3_localstack_secret(
        TYPE s3, 
        scope 's3://testbucket', 
        use_ssl false, 
        key_id 'minioadmin', 
        secret 'minioadmin', 
        url_style 'path', 
        endpoint 'minio:9000',
        region 'us-east-1'
);
