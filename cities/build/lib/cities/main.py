from pyspark.sql.functions import substring, col
from cities.models.cities import Cities
from cities.cities_functions import split_lat_long, get_dept, get_dept_count,\
  departement_udf, departement_fct, compute_distance
import time

def main(spark):
  cities_df = Cities.read(spark)
  Cities.write_parquet(cities_df, "/data/experiment/cities/v1/parquet")
  # manipulation de base
  #cities_clean = cities_df.select("code_postal", "code_commune_insee", "nom_de_la_commune","coordonnees_gps")
  #cities_clean = cities_clean.withColumn("dept", substring("code_postal", 1, 2))
  # test
  cities_clean = cities_df.select("code_postal", "code_commune_insee", "nom_de_la_commune","coordonnees_gps")
  cities_clean = split_lat_long(cities_df, "coordonnees_gps")
  cities_clean = get_dept(cities_clean, "code_postal")
  # aggregation & jointure
  cities_agg = get_dept_count(cities_clean)
  cities_agg = cities_agg.sort(col("nb_cmn").desc())
  Cities.write_csv(cities_agg, "/data/refined/cities/v1/csv")
  # udf
  cities_dept = cities_df.withColumn("dept", departement_udf(cities_df["code_postal"]))
  start = time.time()
  #print(cities_dept.show())
  print("#### udf", time.time()-start) # 
  Cities.write_csv(cities_dept, '/data/refined/departement/v2/csv')
  cities_dept_2 = departement_fct(cities_df)
  start = time.time()
  #print(cities_dept_2.show())
  print("#### sans udf ", time.time()-start) #
  # window
  cities_dist = compute_distance(cities_dept_2)
  print(cities_dist.show())