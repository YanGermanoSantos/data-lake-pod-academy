# Inicializando Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Products Processing") \
    .getOrCreate()

from pyspark.sql.functions import input_file_name, split, col, lit, regexp_replace
from datetime import datetime, timedelta

ts_proc = datetime.now().strftime('%Y%m%d%H%M%S')
ts_proc

t_products = spark.read.option('header','true').csv('s3://pod-academy-lake-370943306683/0000_ingestion/products/products_20260401_20260401070245.csv')
t_products_00 = t_products.withColumn("filename", split(input_file_name(),'/')[5])
t_products_01 = t_products_00.withColumn('ref', split(col('filename'),'_')[1])
t_products_02 = t_products_01.withColumn('ts_file_generation', regexp_replace((split(col('filename'),'_')[2]),'.csv',''))
t_products_03 = t_products_02.withColumn('ts_proc', lit(ts_proc))

t_products_03.createOrReplaceTempView('t_products_03')
t_products_03.cache()
t_products_03.count()

t_products_03.printSchema()

t_products_04 = spark.sql("""
    select
        cast(ref as string) as ref,
        cast(ts_file_generation as string) as ts_file_generation,
        cast(ts_proc as string) as ts_proc,
        cast(proudct_id as string) as product_id,
        cast(product_des as string) as product_des,
        cast(product_category as string) as product_category
    from t_products_03
""")

t_products_04.createOrReplaceTempView('t_products_04')

t_products_04.show(1)

t_products_04.write.partitionBy('ref', 'ts_proc').parquet('s3://pod-academy-lake-370943306683/0001_raw/products/',mode='append')

qtd_registros_sales = t_products_04.count()

t_products_ctl = spark.sql(f"""
    SELECT
        'products' AS assunto,
        ref AS ref,
        ts_file_generation AS ts_file_generation,
        {ts_proc} AS ts_proc,
        {qtd_registros_sales} as qtd_registros
    FROM t_products_04
    LIMIT 1
    
""")

t_products_ctl.createOrReplaceTempView('t_products_ctl')
t_products_ctl.show()

t_products_ctl.write.partitionBy('ref').parquet('s3://pod-academy-lake-370943306683/0003_controle/products/',mode='append')
