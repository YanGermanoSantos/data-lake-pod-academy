# Inicializando Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Users Processing") \
    .getOrCreate()

from pyspark.sql.functions import input_file_name, split, col, lit, regexp_replace
from datetime import datetime, timedelta

ts_proc = datetime.now().strftime('%Y%m%d%H%M%S')
ts_proc

t_users = spark.read.option('header','true').csv('s3://pod-academy-lake-370943306683/0000_ingestion/users/users_20260401_20260401070304.csv')
t_users_00 = t_users.withColumn("filename", split(input_file_name(),'/')[5])
t_users_01 = t_users_00.withColumn('ref', split(col('filename'),'_')[1])
t_users_02 = t_users_01.withColumn('ts_file_generation', regexp_replace((split(col('filename'),'_')[2]),'.csv',''))
t_users_03 = t_users_02.withColumn('ts_proc', lit(ts_proc))

t_users_03.createOrReplaceTempView('t_users_03')
t_users_03.cache()
t_users_03.count()

t_users_03.printSchema()

t_users_04 = spark.sql("""
    select
        cast(ref as string) as ref,
        cast(ts_file_generation as string) as ts_file_generation,
        cast(ts_proc as string) as ts_proc,
        cast(user_id as string) as user_id,
        cast(first_name as string) as first_name,
        cast(last_name as string) as last_name,
        cast(gender as string) as gender,
        cast(email as string) as email
    from t_users_03
""")
t_users_04.createOrReplaceTempView('t_users_04')

t_users_04.show(1)

t_users_04.write.partitionBy('ref', 'ts_proc').parquet('s3://pod-academy-lake-370943306683/0001_raw/users/',mode='append')

qtd_registros_users = t_users_04.count()

t_users_ctl = spark.sql(f"""
    SELECT
        'users' AS assunto,
        ref AS ref,
        ts_file_generation AS ts_file_generation,
        {ts_proc} AS ts_proc,
        {qtd_registros_users} as qtd_registros
    FROM t_users_04
    LIMIT 1
    
""")

t_users_ctl.createOrReplaceTempView('t_users_ctl')
t_users_ctl.show()

t_users_ctl.write.partitionBy('ref').parquet('s3://pod-academy-lake-370943306683/0003_controle/users/',mode='append')
