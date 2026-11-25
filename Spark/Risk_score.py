"""
Client Behavioral Profile + Risk Score (PySpark, local[*]).

Источник  — ClickHouse MART:
    clean.transactions
    risk.suspicious_events
    core.clients / core.cards / core.devices
Приёмник  — ClickHouse risk.client_behavior_profile
            MinIO s3a://risk-analytics/client_risk_YYYY_MM_DD.parquet
                  s3a://risk-analytics/high_risk_clients_YYYY_MM_DD.parquet
                  s3a://risk-investigations/investigation_YYYY_MM_DD.parquet

Секреты — только из окружения (docker exec -e ...).
"""
import os
import datetime

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T
from py4j.java_gateway import java_import


# ============================================================
# 0. Секреты — обязательны, дефолтов нет
# ============================================================
def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(
            f"Переменная окружения '{name}' не задана! "
            f"Убедитесь, что она проброшена через docker exec -e."
        )
    return value


CH_USER      = get_required_env("CLICKHOUSE_USER")
CH_PASS      = get_required_env("CLICKHOUSE_PASSWORD")
MINIO_ACCESS = get_required_env("MINIO_ACCESS_KEY")
MINIO_SECRET = get_required_env("MINIO_SECRET_KEY")


# ============================================================
# 1. Spark (local[*] + MinIO)
# ============================================================
spark = SparkSession.builder \
    .appName("ClientBehaviorProfile") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

jdbc_url = "jdbc:clickhouse://clickhouse:8123/default"
jdbc_props = {
    "user": CH_USER,
    "password": CH_PASS,
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}


# ============================================================
# 2. Чтение данных из ClickHouse MART
# ============================================================
print("Чтение данных из ClickHouse MART...")
df_tx  = spark.read.jdbc(url=jdbc_url, table="clean.transactions",       properties=jdbc_props)
df_sus = spark.read.jdbc(url=jdbc_url, table="risk.suspicious_events",   properties=jdbc_props)

# нормализованные типы (ClickHouse JDBC отдаёт DateTime как timestamp)
df_tx  = df_tx.withColumn("event_ts",  F.to_timestamp("event_datetime"))
df_sus = df_sus.withColumn("event_ts", F.to_timestamp("event_datetime"))


# ============================================================
# 3. «Текущая» дата —  (max(event_datetime))
# ============================================================
max_row = df_tx.agg(F.max("event_ts")).collect()[0]
NOW_TS = max_row[0]
print(f"Расчетная текущая дата (max event_datetime): {NOW_TS}")

# строковые cutoff-ы — чтобы избежать проблем с типизацией в F.expr
now_str   = NOW_TS.strftime("%Y-%m-%d %H:%M:%S")
d1_str    = (NOW_TS - datetime.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
d7_str    = (NOW_TS - datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
d30_str   = (NOW_TS - datetime.timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

NOW_DATE = NOW_TS.date()


# ============================================================
# 4. Окна: 24h / 7d / 30d
# ============================================================
df_tx_24h  = df_tx.filter(F.col("event_ts") > F.lit(d1_str).cast("timestamp"))
df_tx_7d   = df_tx.filter(F.col("event_ts") > F.lit(d7_str).cast("timestamp"))
df_tx_30d  = df_tx.filter(F.col("event_ts") > F.lit(d30_str).cast("timestamp"))

df_sus_7d  = df_sus.filter(F.col("event_ts") > F.lit(d7_str).cast("timestamp"))
df_sus_30d = df_sus.filter(F.col("event_ts") > F.lit(d30_str).cast("timestamp"))


# ============================================================
# 5. Базовые агрегаты (транзакции)
# ============================================================
df_24h = df_tx_24h.groupBy("client_id").agg(
    F.count("*").cast("int").alias("transactions_24h"),
    F.round(F.max("amount"), 2).alias("max_amount_24h"),
    F.count(F.when(F.col("status") == "declined", True)).cast("int").alias("declined_transactions_24h"),
)

df_7d = df_tx_7d.groupBy("client_id").agg(
    F.count("*").cast("int").alias("transactions_7d"),
    F.count(F.when(F.col("country") != "RU", True)).cast("int").alias("international_transactions_7d"),
)

df_30d = df_tx_30d.groupBy("client_id").agg(
    F.count("*").cast("int").alias("transactions_30d"),
    F.round(F.avg("amount"), 2).alias("avg_amount_30d"),
    F.round(F.expr("percentile_approx(amount, 0.5, 10000)"), 2).alias("median_amount_30d"),
    F.round(F.max("amount"), 2).alias("max_amount_30d"),
    F.round(F.stddev("amount"), 2).alias("std_amount_30d"),
    F.countDistinct("merchant_id").cast("int").alias("unique_merchants_30d"),
    F.countDistinct("device_id").cast("int").alias("unique_devices_30d"),
    F.countDistinct("country").cast("int").alias("unique_countries_30d"),
    F.countDistinct("city").cast("int").alias("unique_cities_30d"),
    F.count(F.when(F.hour("event_ts").between(0, 5), True)).cast("int").alias("night_transactions_30d"),
)


# ============================================================
# 6. new_device_24h — устройство, которого не было до вчера
# ============================================================
df_known_dev = df_tx \
    .filter(F.col("event_ts") <= F.lit(d1_str).cast("timestamp")) \
    .filter(F.col("device_id") != "") \
    .select("client_id", "device_id").distinct()

df_dev_24h = df_tx_24h \
    .filter(F.col("device_id") != "") \
    .select("client_id", "device_id").distinct()

df_new_dev = df_dev_24h.join(df_known_dev, ["client_id", "device_id"], "left_anti") \
    .groupBy("client_id").count() \
    .withColumn("new_device_24h", F.when(F.col("count") > 0, True).otherwise(False)) \
    .select("client_id", "new_device_24h")


# ============================================================
# 7. Срабатывания правил (risk.suspicious_events)
# ============================================================
df_rules_7d = df_sus_7d.groupBy("client_id").agg(
    F.count(F.when(F.col("rule_name") == "high_velocity", True)).cast("int").alias("high_velocity_events_7d"),
)

df_rules_30d = df_sus_30d.groupBy("client_id").agg(
    F.count(F.when(F.col("rule_name") == "impossible_travel",       True)).cast("int").alias("impossible_travel_events_30d"),
    F.count(F.when(F.col("rule_name") == "cashout_pattern",         True)).cast("int").alias("cashout_events_30d"),
    F.count(F.when(F.col("rule_name") == "card_testing",            True)).cast("int").alias("card_testing_events_30d"),
    F.count(F.when(F.col("rule_name") == "large_amount",            True)).cast("int").alias("large_amount_events_30d"),
    F.count(F.when(F.col("rule_name") == "new_device_large_amount", True)).cast("int").alias("new_device_large_events_30d"),
)


# ============================================================
# 8. Сборка профиля
# ============================================================
df_profile = df_30d \
    .join(df_7d,      "client_id", "left") \
    .join(df_24h,     "client_id", "left") \
    .join(df_new_dev, "client_id", "left") \
    .join(df_rules_7d,  "client_id", "left") \
    .join(df_rules_30d, "client_id", "left")

bool_cols = ["new_device_24h"]
num_cols = [
    "transactions_24h", "transactions_7d", "transactions_30d",
    "avg_amount_30d", "median_amount_30d", "max_amount_30d", "std_amount_30d",
    "max_amount_24h",
    "unique_merchants_30d", "unique_devices_30d", "unique_countries_30d", "unique_cities_30d",
    "night_transactions_30d", "declined_transactions_24h", "international_transactions_7d",
    "high_velocity_events_7d",
    "impossible_travel_events_30d", "cashout_events_30d",
    "card_testing_events_30d", "large_amount_events_30d", "new_device_large_events_30d",
]

df_profile = df_profile.fillna(0, subset=num_cols).fillna(False, subset=bool_cols)


# ============================================================
# 9. Risk Score 0–100
#    +25 impossible_travel
#    +20 high_velocity
#    +20 cashout
#    +15 new_device + large_amount
#    +10 multiple_declines
#    +10 unusual_country
# ============================================================
df_profile = df_profile.withColumn(
    "risk_score",
    F.expr("""
        greatest(0, least(100,
              (CASE WHEN impossible_travel_events_30d > 0 THEN 25 ELSE 0 END)
            + (CASE WHEN high_velocity_events_7d     > 0 THEN 20 ELSE 0 END)
            + (CASE WHEN cashout_events_30d          > 0 THEN 20 ELSE 0 END)
            + (CASE WHEN new_device_large_events_30d > 0 THEN 15 ELSE 0 END)
            + (CASE WHEN declined_transactions_24h  >= 3 THEN 10 ELSE 0 END)
            + (CASE WHEN international_transactions_7d > 0 THEN 10 ELSE 0 END)
            + (CASE WHEN card_testing_events_30d     > 0 THEN 10 ELSE 0 END)
        ))
    """).cast("int"),
).withColumn(
    "risk_level",
    F.when(F.col("risk_score") >= 80, "critical")
     .when(F.col("risk_score") >= 50, "high")
     .when(F.col("risk_score") >= 25, "medium")
     .otherwise("low"),
).withColumn(
    "profile_date", F.lit(NOW_DATE).cast("date"),
)


# ============================================================
# 10. Финальный набор колонок
# ============================================================
final_cols = [
    "client_id", "profile_date",
    "transactions_24h", "transactions_7d", "transactions_30d",
    "avg_amount_30d", "median_amount_30d", "max_amount_30d", "std_amount_30d",
    "max_amount_24h",
    "unique_merchants_30d", "unique_devices_30d", "unique_countries_30d", "unique_cities_30d",
    "night_transactions_30d", "declined_transactions_24h",
    "new_device_24h", "international_transactions_7d",
    "high_velocity_events_7d", "impossible_travel_events_30d", "cashout_events_30d",
    "card_testing_events_30d", "large_amount_events_30d", "new_device_large_events_30d",
    "risk_score", "risk_level",
]

df_final = df_profile.select(*final_cols)
print("Схема итогового профиля:")
df_final.printSchema()


# ============================================================
# 11. Запись в ClickHouse
# ============================================================
print("Запись в ClickHouse risk.client_behavior_profile...")
df_final.write.jdbc(
    url=jdbc_url,
    table="risk.client_behavior_profile",
    mode="append",
    properties=jdbc_props,
)


# ============================================================
# 12. Запись в MinIO 
# ============================================================
def write_single_parquet(df, bucket: str, filename: str):
    tmp_path = f"s3a://{bucket}/tmp_{filename}"
    df.coalesce(1).write.format("parquet").mode("overwrite").save(tmp_path)

    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.FileSystem.get(spark._jvm.URI(f"s3a://{bucket}"), hadoop_conf)

    src_dir  = spark._jvm.Path(tmp_path)
    dst_file = spark._jvm.Path(f"s3a://{bucket}/{filename}")

    for st in fs.listStatus(src_dir):
        name = st.getPath().getName()
        if name.startswith("part-"):
            fs.rename(st.getPath(), dst_file)
            break

    fs.delete(src_dir, True)
    print(f"Файл сохранён в MinIO: s3a://{bucket}/{filename}")


java_import(spark._jvm, "org.apache.hadoop.fs.Path")
java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
java_import(spark._jvm, "java.net.URI")

date_str = NOW_TS.strftime("%Y_%m_%d")
today_str = datetime.datetime.now().strftime("%Y_%m_%d")  

print("Запись в MinIO: risk-analytics...")
write_single_parquet(df_final, "risk-analytics", f"client_risk_{date_str}.parquet")

df_high = df_final.filter(F.col("risk_score") >= 80)
if df_high.count() > 0:
    write_single_parquet(df_high, "risk-analytics", f"high_risk_clients_{date_str}.parquet")
# ============================================================
# 13. Расследование (часть X) → risk-investigations
# ============================================================
df_inv = df_high.withColumn(
    "probable_scenario",
    F.when(F.col("cashout_events_30d") > 0, "cashout")
     .when(
         (F.col("new_device_large_events_30d") > 0)
         & (F.col("impossible_travel_events_30d") > 0),
         "account_takeover",
     )
     .when(F.col("card_testing_events_30d") > 0, "card_testing")
     .when(F.col("high_velocity_events_7d") > 0, "velocity_abuse")
     .otherwise("other"),
).withColumn("investigation_date", F.lit(NOW_DATE).cast("date")) \
 .select(
    "client_id", "investigation_date", "probable_scenario",
    "risk_score", "risk_level",
    "impossible_travel_events_30d", "high_velocity_events_7d",
    "cashout_events_30d", "new_device_large_events_30d", "card_testing_events_30d",
 )

if df_inv.count() > 0:
    print("Запись в MinIO: risk-investigations...")
    write_single_parquet(df_inv, "risk-investigations", f"investigation_{date_str}.parquet")
    
spark.stop()
print("done")
