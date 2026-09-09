from pyspark import pipelines as dp
from pyspark.sql.functions import col, split, explode, trim, count, avg, round as spark_round

# ---------- BRONZE ----------
@dp.table(
    name="jobs_bronze",
    comment="Ingestion brute des offres d'emploi déposées dans le volume landing"
)
def jobs_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/tech_jobs_market/landing/raw_files/")
    )

# ---------- SILVER ----------
@dp.table(
    name="jobs_silver",
    comment="Offres nettoyées : salaires valides uniquement, compétences éclatées en lignes"
)
@dp.expect_or_drop("salaire_valide", "salary_min IS NOT NULL AND salary_max IS NOT NULL")
@dp.expect_or_drop("salaire_coherent", "salary_max >= salary_min")
@dp.expect("ville_renseignee", "city IS NOT NULL")
def jobs_silver():
    return (
        dp.read_stream("jobs_bronze")
        .withColumn("skill", explode(split(col("required_skills"), ";")))
        .withColumn("skill", trim(col("skill")))
        .drop("required_skills")
    )

# ---------- GOLD ----------
@dp.materialized_view(
    name="top_skills",
    comment="Classement des compétences les plus demandées dans les offres"
)
def top_skills():
    return (
        dp.read("jobs_silver")
        .groupBy("skill")
        .agg(count("*").alias("nb_offres"))
        .orderBy(col("nb_offres").desc())
    )

@dp.materialized_view(
    name="salary_by_level",
    comment="Salaire moyen (min/max) par niveau d'expérience"
)
def salary_by_level():
    return (
        dp.read("jobs_silver")
        .groupBy("experience_level")
        .agg(
            spark_round(avg("salary_min"), 0).alias("salaire_min_moyen"),
            spark_round(avg("salary_max"), 0).alias("salaire_max_moyen"),
            count("*").alias("nb_offres")
        )
    )

@dp.materialized_view(
    name="jobs_by_city",
    comment="Nombre d'offres par ville, avec part du télétravail"
)
def jobs_by_city():
    return (
        dp.read("jobs_silver")
        .groupBy("city", "remote")
        .agg(count("*").alias("nb_offres"))
        .orderBy(col("nb_offres").desc())
    )