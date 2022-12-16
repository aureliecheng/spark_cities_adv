class Cities:
  CODE_COMMUNE_INSEE= "code_commune_insee",
  NOM_COMMUNE = "nom_de_la_commune",
  CODE_POSTAL = "code_postal",
  LIGNE_5 = "ligne_5",
  LIB_ACHEMINEMENT = "libelle_d_acheminement",
  COORDONNEES_GPS = "coordonnees_gps"

  @staticmethod
  def read(spark):
    return spark.read.csv("hdfs://localhost:9000/data/raw/cities/v1/csv/laposte_hexasmal.csv",
    header=True, sep=";")

  @staticmethod
  def write_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

  @staticmethod
  def write_csv(df, path):
    df.write.mode("overwrite").csv(path)