from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Book Processing") \
    .getOrCreate()

from pyspark.sql.functions import input_file_name, split, col, lit, regexp_replace
from datetime import datetime, timedelta

dt_exec_book = datetime.now().date().strftime("%Y%m%d")
ts_proc = datetime.now().strftime('%Y%m%d%H%M%S')
ref_ini = (datetime.now().date() - timedelta(days=180)).strftime("%Y%m%d")
dt_exec_book, ref_ini

# Leitura Sales
t_sales = spark.read.parquet('s3://pod-academy-lake-370943306683/0001_raw/sales/')
t_sales_00 = t_sales.where(f'ref >= {ref_ini}')
t_sales_00.createOrReplaceTempView('t_sales_00')

t_sales_00.count()

# Deduplicacao
t_sales_dedup_00 = spark.sql("""
    select
        concat(ts_proc, ts_file_generation) as dedup_key,
        *
    from t_sales_00
""")
t_sales_dedup_00.createOrReplaceTempView('t_sales_dedup_00')

spark.sql("""
    select
    dedup_key,
    count(*)
    from t_sales_dedup_00
    group by 1
""").show(truncate=False)

t_sales_dedup_01 = spark.sql("""
    select
        max(dedup_key) as max_dedup_key
    from t_sales_dedup_00
""")
t_sales_dedup_01.createOrReplaceTempView('t_sales_dedup_01')

t_sales_dedup_02 = spark.sql("""
    select
        a.*
    from t_sales_dedup_00 a
    inner join t_sales_dedup_01 b
    on a.dedup_key = b.max_dedup_key
""")
t_sales_dedup_02.show(1)
t_sales_dedup_02.createOrReplaceTempView('t_sales_dedup_02')

## Leitura Produtos
t_products = spark.read.parquet('s3://pod-academy-lake-370943306683/0001_raw/products/')
t_products.createOrReplaceTempView('t_products')

t_products_dedup_00 = spark.sql("""
    select
        concat(ts_file_generation, ts_proc) dedup_key,
        *
    from t_products
""")
t_products_dedup_00.createOrReplaceTempView('t_products_dedup_00')

t_products_dedup_01 = spark.sql("""
    select
        max(dedup_key) max_dedup_key
    from t_products_dedup_00
""")
t_products_dedup_01.createOrReplaceTempView('t_products_dedup_01')

t_products_dedup_02 = spark.sql("""
    select
        a.*
    from t_products_dedup_00 a
    inner join t_products_dedup_01 b
    on a.dedup_key = b.max_dedup_key
""")
t_products_dedup_02.createOrReplaceTempView('t_products_dedup_02')
t_products_dedup_02.count()

t_products_dedup_02.show(1)

t_sales_products_join = spark.sql("""
    select
        a.*,
        b.product_category
    from t_sales_dedup_02 a
    left join t_products_dedup_02 b
    on a.product_id = b.product_id
""")
t_sales_products_join.createOrReplaceTempView('t_sales_products_join')

month_flags = spark.sql(f"""
    select
        transaction_id,
        user_id,
        ref,
        product_category,
        dat_creation,
        amount,
        payment_method,
        (case when int(months_between(to_date('{dt_exec_book}','yyyyMMdd'), to_date(CAST(ref AS STRING), 'yyyyMMdd'))) <= 1 then 1 else 0 end) flag_u1m,
        (case when int(months_between(to_date('{dt_exec_book}','yyyyMMdd'), to_date(CAST(ref AS STRING), 'yyyyMMdd'))) <= 3 then 1 else 0 end) flag_u3m,
        (case when int(months_between(to_date('{dt_exec_book}','yyyyMMdd'), to_date(CAST(ref AS STRING), 'yyyyMMdd'))) <= 6 then 1 else 0 end) flag_u6m
    from t_sales_products_join
""")
month_flags.createOrReplaceTempView('month_flags')

bk_00 = spark.sql(f"""
    select
        user_id,
        '{dt_exec_book}' ref,
        '{ts_proc}' ts_proc,
        sum(case when product_category = 'Livros' then 1 end) qtd_pcat_livros,
        sum(case when product_category = 'Roupas' then 1 end) qtd_pcat_roupas,
        sum(case when product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr,
        sum(case when product_category = 'Jogos' then 1 end) qtd_pcat_jogos,
        
        sum(case when flag_u1m = 1 and product_category = 'Livros' then 1 end) qtd_pcat_livros_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Roupas' then 1 end) qtd_pcat_roupas_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Jogos' then 1 end) qtd_pcat_jogos_u1m,
        
        sum(case when flag_u3m = 1 and product_category = 'Livros' then 1 end) qtd_pcat_livros_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Roupas' then 1 end) qtd_pcat_roupas_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Jogos' then 1 end) qtd_pcat_jogos_u3m,
        
        sum(case when flag_u6m = 1 and product_category = 'Livros' then 1 end) qtd_pcat_livros_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Roupas' then 1 end) qtd_pcat_roupas_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Eletronicos' then 1 end) qtd_pcat_eletr_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Jogos' then 1 end) qtd_pcat_jogos_u6m,
        
        sum(case when product_category = 'Livros' then amount end) sum_pcat_livros,
        sum(case when product_category = 'Roupas' then amount end) sum_pcat_roupas,
        sum(case when product_category = 'Eletronicos' then amount end) sum_pcat_eletr,
        sum(case when product_category = 'Jogos' then amount end) sum_pcat_jogos,
        
        sum(case when flag_u1m = 1 and product_category = 'Livros' then amount end) sum_pcat_livros_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) sum_pcat_roupas_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) sum_pcat_eletr_u1m,
        sum(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) sum_pcat_jogos_u1m,
        
        sum(case when flag_u3m = 1 and product_category = 'Livros' then amount end) sum_pcat_livros_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) sum_pcat_roupas_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) sum_pcat_eletr_u3m,
        sum(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) sum_pcat_jogos_u3m,
        
        sum(case when flag_u6m = 1 and product_category = 'Livros' then amount end) sum_pcat_livros_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) sum_pcat_roupas_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) sum_pcat_eletr_u6m,
        sum(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) sum_pcat_jogos_u6m,
        
        avg(case when product_category = 'Livros' then amount end) avg_pcat_livros,
        avg(case when product_category = 'Roupas' then amount end) avg_pcat_roupas,
        avg(case when product_category = 'Eletronicos' then amount end) avg_pcat_eletr,
        avg(case when product_category = 'Jogos' then amount end) avg_pcat_jogos,
        
        avg(case when flag_u1m = 1 and product_category = 'Livros' then amount end) avg_pcat_livros_u1m,
        avg(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) avg_pcat_roupas_u1m,
        avg(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) avg_pcat_eletr_u1m,
        avg(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) avg_pcat_jogos_u1m,
        
        avg(case when flag_u3m = 1 and product_category = 'Livros' then amount end) avg_pcat_livros_u3m,
        avg(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) avg_pcat_roupas_u3m,
        avg(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) avg_pcat_eletr_u3m,
        avg(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) avg_pcat_jogos_u3m,
        
        avg(case when flag_u6m = 1 and product_category = 'Livros' then amount end) avg_pcat_livros_u6m,
        avg(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) avg_pcat_roupas_u6m,
        avg(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) avg_pcat_eletr_u6m,
        avg(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) avg_pcat_jogos_u6m,
        
        min(case when product_category = 'Livros' then amount end) min_pcat_livros,
        min(case when product_category = 'Roupas' then amount end) min_pcat_roupas,
        min(case when product_category = 'Eletronicos' then amount end) min_pcat_eletr,
        min(case when product_category = 'Jogos' then amount end) min_pcat_jogos,
        
        min(case when flag_u1m = 1 and product_category = 'Livros' then amount end) min_pcat_livros_u1m,
        min(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) min_pcat_roupas_u1m,
        min(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) min_pcat_eletr_u1m,
        min(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) min_pcat_jogos_u1m,
        
        min(case when flag_u3m = 1 and product_category = 'Livros' then amount end) min_pcat_livros_u3m,
        min(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) min_pcat_roupas_u3m,
        min(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) min_pcat_eletr_u3m,
        min(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) min_pcat_jogos_u3m,
        
        min(case when flag_u6m = 1 and product_category = 'Livros' then amount end) min_pcat_livros_u6m,
        min(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) min_pcat_roupas_u6m,
        min(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) min_pcat_eletr_u6m,
        min(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) min_pcat_jogos_u6m,
        
        max(case when product_category = 'Livros' then amount end) max_pcat_livros,
        max(case when product_category = 'Roupas' then amount end) max_pcat_roupas,
        max(case when product_category = 'Eletronicos' then amount end) max_pcat_eletr,
        max(case when product_category = 'Jogos' then amount end) max_pcat_jogos,
        
        max(case when flag_u1m = 1 and product_category = 'Livros' then amount end) max_pcat_livros_u1m,
        max(case when flag_u1m = 1 and product_category = 'Roupas' then amount end) max_pcat_roupas_u1m,
        max(case when flag_u1m = 1 and product_category = 'Eletronicos' then amount end) max_pcat_eletr_u1m,
        max(case when flag_u1m = 1 and product_category = 'Jogos' then amount end) max_pcat_jogos_u1m,
        
        max(case when flag_u3m = 1 and product_category = 'Livros' then amount end) max_pcat_livros_u3m,
        max(case when flag_u3m = 1 and product_category = 'Roupas' then amount end) max_pcat_roupas_u3m,
        max(case when flag_u3m = 1 and product_category = 'Eletronicos' then amount end) max_pcat_eletr_u3m,
        max(case when flag_u3m = 1 and product_category = 'Jogos' then amount end) max_pcat_jogos_u3m,
        
        max(case when flag_u6m = 1 and product_category = 'Livros' then amount end) max_pcat_livros_u6m,
        max(case when flag_u6m = 1 and product_category = 'Roupas' then amount end) max_pcat_roupas_u6m,
        max(case when flag_u6m = 1 and product_category = 'Eletronicos' then amount end) max_pcat_eletr_u6m,
        max(case when flag_u6m = 1 and product_category = 'Jogos' then amount end) max_pcat_jogos_u6m,
        
        sum(case when payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd,
        sum(case when payment_method = 'PIX' then 1 end) qtd_pm_pix,
        sum(case when payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc,
        sum(case when payment_method = 'Boleto' then 1 end) qtd_pm_boleto,
        
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'PIX' then 1 end) qtd_pm_pix_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Boleto' then 1 end) qtd_pm_boleto_u1m,
        
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'PIX' then 1 end) qtd_pm_pix_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Boleto' then 1 end) qtd_pm_boleto_u3m,
        
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then 1 end) qtd_pm_cd_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'PIX' then 1 end) qtd_pm_pix_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then 1 end) qtd_pm_cc_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Boleto' then 1 end) qtd_pm_boleto_u6m,
        
        sum(case when payment_method = 'Cartao Debito' then amount end) sum_pm_cd,
        sum(case when payment_method = 'PIX' then amount end) sum_pm_pix,
        sum(case when payment_method = 'Cartao Credito' then amount end) sum_pm_cc,
        sum(case when payment_method = 'Boleto' then amount end) sum_pm_boleto,
        
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) sum_pm_cd_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) sum_pm_pix_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) sum_pm_cc_u1m,
        sum(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) sum_pm_boleto_u1m,
        
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) sum_pm_cd_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) sum_pm_pix_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) sum_pm_cc_u3m,
        sum(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) sum_pm_boleto_u3m,
        
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) sum_pm_cd_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) sum_pm_pix_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) sum_pm_cc_u6m,
        sum(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) sum_pm_boleto_u6m,
        
        min(case when payment_method = 'Cartao Debito' then amount end) min_pm_cd,
        min(case when payment_method = 'PIX' then amount end) min_pm_pix,
        min(case when payment_method = 'Cartao Credito' then amount end) min_pm_cc,
        min(case when payment_method = 'Boleto' then amount end) min_pm_boleto,
        
        min(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) min_pm_cd_u1m,
        min(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) min_pm_pix_u1m,
        min(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) min_pm_cc_u1m,
        min(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) min_pm_boleto_u1m,
        
        min(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) min_pm_cd_u3m,
        min(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) min_pm_pix_u3m,
        min(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) min_pm_cc_u3m,
        min(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) min_pm_boleto_u3m,
        
        min(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) min_pm_cd_u6m,
        min(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) min_pm_pix_u6m,
        min(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) min_pm_cc_u6m,
        min(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) min_pm_boleto_u6m,
        
        max(case when payment_method = 'Cartao Debito' then amount end) max_pm_cd,
        max(case when payment_method = 'PIX' then amount end) max_pm_pix,
        max(case when payment_method = 'Cartao Credito' then amount end) max_pm_cc,
        max(case when payment_method = 'Boleto' then amount end) max_pm_boleto,
        
        max(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) max_pm_cd_u1m,
        max(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) max_pm_pix_u1m,
        max(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) max_pm_cc_u1m,
        max(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) max_pm_boleto_u1m,
        
        max(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) max_pm_cd_u3m,
        max(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) max_pm_pix_u3m,
        max(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) max_pm_cc_u3m,
        max(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) max_pm_boleto_u3m,
        
        max(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) max_pm_cd_u6m,
        max(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) max_pm_pix_u6m,
        max(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) max_pm_cc_u6m,
        max(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) max_pm_boleto_u6m,
        
        avg(case when payment_method = 'Cartao Debito' then amount end) avg_pm_cd,
        avg(case when payment_method = 'PIX' then amount end) avg_pm_pix,
        avg(case when payment_method = 'Cartao Credito' then amount end) avg_pm_cc,
        avg(case when payment_method = 'Boleto' then amount end) avg_pm_boleto,
        
        avg(case when flag_u1m = 1 and payment_method = 'Cartao Debito' then amount end) avg_pm_cd_u1m,
        avg(case when flag_u1m = 1 and payment_method = 'PIX' then amount end) avg_pm_pix_u1m,
        avg(case when flag_u1m = 1 and payment_method = 'Cartao Credito' then amount end) avg_pm_cc_u1m,
        avg(case when flag_u1m = 1 and payment_method = 'Boleto' then amount end) avg_pm_boleto_u1m,
        
        avg(case when flag_u3m = 1 and payment_method = 'Cartao Debito' then amount end) avg_pm_cd_u3m,
        avg(case when flag_u3m = 1 and payment_method = 'PIX' then amount end) avg_pm_pix_u3m,
        avg(case when flag_u3m = 1 and payment_method = 'Cartao Credito' then amount end) avg_pm_cc_u3m,
        avg(case when flag_u3m = 1 and payment_method = 'Boleto' then amount end) avg_pm_boleto_u3m,
        
        avg(case when flag_u6m = 1 and payment_method = 'Cartao Debito' then amount end) avg_pm_cd_u6m,
        avg(case when flag_u6m = 1 and payment_method = 'PIX' then amount end) avg_pm_pix_u6m,
        avg(case when flag_u6m = 1 and payment_method = 'Cartao Credito' then amount end) avg_pm_cc_u6m,
        avg(case when flag_u6m = 1 and payment_method = 'Boleto' then amount end) avg_pm_boleto_u6m
        
    from month_flags
    group by 1
""")
bk_00.createOrReplaceTempView('bk_00')

bk_00.write.partitionBy('ref','ts_proc').parquet('s3://pod-academy-lake-370943306683/0002_curated/book_1_fraude/',mode='append')

qtd_book = bk_00.count()
book_ctl = spark.sql(f"""
    select
        'Book_1_Fraude' as assunto,
        {dt_exec_book} as ref,
        {ts_proc} as ts_proc,
        {qtd_book} as qtd_registros
""")

# Write ctl file
book_ctl.write.partitionBy('ref').parquet('s3://pod-academy-lake-370943306683/0003_controle/book/book_1_fraude/',mode='append')
