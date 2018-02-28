
input=/input/aaa.txt
output=/output

#hadoop fs -rm -r -skipTrash $output

spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 1 \
    --executor-memory 1G \
    --executor-cores 1 \
    --class com.xwtech.test.Test1 target/scala-2.11/hbase-project_2.11-1.0.jar \
    $input $output
