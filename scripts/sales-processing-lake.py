# Inicializando Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Sales Processing") \
    .getOrCreate()

from pyspark.sql.functions import input_file_name, split, col, lit, regexp_replace
from datetime import datetime, timedelta



ts_proc = datetime.now().strftime('%Y%m%d%H%M%S')
ts_proc

t_sales = spark.read.option('header','true').csv('s3://pod-academy-lake-370943306683/0000_ingestion/sales/*.csv')
t_sales_00 = t_sales.withColumn("filename", split(input_file_name(),'/')[5])
t_sales_01 = t_sales_00.withColumn('ref', split(col('filename'),'_')[1])
t_sales_02 = t_sales_01.withColumn('ts_file_generation', regexp_replace((split(col('filename'),'_')[2]),'.csv',''))
t_sales_03 = t_sales_02.withColumn('ts_proc', lit(ts_proc))

t_sales_03.createOrReplaceTempView('t_sales_03')
t_sales_03.cache()
t_sales_03.count()

t_sales_03.printSchema()

t_sales_04 = spark.sql("""
    SELECT
        CAST(replace(substr(dat_creation,1,10),'-','') as string) ref,
        CAST(ts_proc as string) as ts_proc,
        CAST(ts_file_generation as string) ts_file_generation,
        CAST(transaction_id as string) as transaction_id,
        CAST(user_id as string) as user_id,
        CAST(product_id as string) as product_id,
        CAST(dat_creation as timestamp) as dat_creation,
        CAST(amount as decimal(14,2)) as amount,
        CAST(quantity as int) as quantity,
        CAST(payment_method as string) as payment_method
    FROM t_sales_03


""")

t_sales_04.createOrReplaceTempView('t_sales_04')

t_sales_04.show(1)

t_sales_04.write.partitionBy('ref', 'ts_proc').parquet('s3://pod-academy-lake-370943306683/0001_raw/sales/',mode='append')

qtd_registros_sales = t_sales_04.count()

t_sales_ctl = spark.sql(f"""
    SELECT
        'sales' AS assunto,
        ref AS ref,
        ts_file_generation AS ts_file_generation,
        {ts_proc} AS ts_proc,
        {qtd_registros_sales} as qtd_registros
    FROM t_sales_04
    LIMIT 1
    
""")

t_sales_ctl.createOrReplaceTempView('t_sales_ctl')
t_sales_ctl.show()

t_sales_ctl.write.partitionBy('ref').parquet('s3://pod-academy-lake-370943306683/0003_controle/sales/',mode='append')
