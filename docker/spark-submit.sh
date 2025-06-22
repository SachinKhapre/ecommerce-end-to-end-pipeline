export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.2 \
    --master local[*] \
    spark_job/spark-process-orders.py