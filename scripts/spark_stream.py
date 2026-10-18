from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, length, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialisation de Spark avec les connecteurs Kafka et PostgreSQL
spark = SparkSession.builder \
    .appName("MastodonAdvancedStreamProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.2.18") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    s.connect(("projet_5spark-kafka-1", 9092))
    print("Connexion TCP réussie à Kafka !")
except Exception as e:
    print(f"Échec de la connexion : {e}")
finally:
    s.close()

# Définition du schéma JSON attendu
schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("favourites_count", IntegerType(), True),
    StructField("reblogs_count", IntegerType(), True)
])

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.22.0.4:9092") \
    .option("subscribe", "mastodon_stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Désérialisation du JSON
parsed_df = df_raw.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"))) \
    .withColumn("toot_length", length(col("text")))

# Transformation 1 : Filtrage des toots (ex: mots-clés pertinents ou non nuls)
filtered_df = parsed_df.filter(
    col("text").isNotNull() & 
    (col("text").contains("AI") | col("text").contains("Data") | col("text").contains("Python"))
)

# Transformation 2 & Action 1 : Fenêtre temporelle (1 minute) et comptage par utilisateur / fenêtre
windowed_counts = filtered_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("user_id")
    ) \
    .count()

# Action 2 : Statistiques (longueur moyenne des toots par utilisateur sur une fenêtre de 5 minutes)
user_stats = filtered_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    ) \
    .agg(avg("toot_length").alias("avg_toot_length"))

# 5. APLATISSEMENT DES COLONNES WINDOW (POUR POSTGRESQL)
# ==========================================
windowed_counts_for_pg = windowed_counts \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

user_stats_for_pg = user_stats \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

# Fonction pour écrire chaque micro-lot dans PostgreSQL
def write_to_postgres(batch_df, batch_id, table_name):
    try:
        count = batch_df.count()
        if count > 0:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/mastodon_db") \
                .option("dbtable", table_name) \
                .option("user", "postgres") \
                .option("password", "postgres") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print(f"[{table_name}] Batch {batch_id} écrit avec succès ({count} lignes).")
    except Exception as e:
        print(f"[{table_name}] Erreur Batch {batch_id}: {str(e)}")

# Flux 1 : Toots filtrés
query_filtered = filtered_df.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "toots_filtered")) \
    .outputMode("append") \
    .start()

# Flux 2 : Comptage par fenêtre aplati
query_counts = windowed_counts_for_pg.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "windowed_counts")) \
    .outputMode("update") \
    .start()

# Flux 3 : Statistiques par fenêtre aplaties
query_stats = user_stats_for_pg.writeStream \
    .foreachBatch(lambda df, id: write_to_postgres(df, id, "user_toot_stats")) \
    .outputMode("update") \
    .start()

print("Pipeline de streaming avancé démarré avec succès et prêt pour PostgreSQL !")