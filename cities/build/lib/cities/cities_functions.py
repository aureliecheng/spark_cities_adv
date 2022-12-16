from pyspark.sql.functions import col, split, substring, udf, when, min as ps_min
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql.window import Window
from geopy import distance

def split_lat_long(df, lat_long_col):
    df = df.withColumn("latitude", split(col(lat_long_col), ",").getItem(0).cast(DoubleType()))\
            .withColumn("longitude", split(col(lat_long_col), ",").getItem(1).cast(DoubleType()))\
            .drop(lat_long_col)
    return df

def get_dept(df, cp):
    return df.withColumn("dept", substring(cp, 1, 2)).drop(cp)

# agg & jointure
def get_dept_count(df):
    df_count = df.distinct()\
            .groupBy("dept")\
            .count()\
            .withColumnRenamed("count", "nb_cmn")
    return df.join(df_count, ["dept"], "inner")
    
# udf
def departement(cp):
    #corse
    if cp.startswith("20"):
        if int(cp) < 20200:
            return "2A"
        return "2B"
    return cp[:2]

departement_udf = udf(departement, StringType())

def departement_fct(df):
    return df.withColumn("dept", when(col("code_postal").startswith("20"), 
                                        when(col("code_postal").cast(IntegerType()) < 20200, "2A").otherwise("2B"))
                                        .otherwise(substring("code_postal", 1, 2)))

# prefecture du dept se situe dans la ville ayant le code postal le plus petit dans tout le dept
def find_prefecture(df):
    gps = df.select(col("code_postal").alias("cp"), col("coordonnees_gps").alias("gps_prefecture"))\
            .distinct()
    w = Window.partitionBy('dept')
    df = df.withColumn("prefecture", ps_min(col("code_postal")).over(w))
    return df.join(gps, df["prefecture"]==gps["cp"], "left")

def to_tuple(val):
    t = val.split(",")
    x = [i for i in t]
    return tuple(x)

to_tuple_udf = udf(to_tuple)

def get_distance(lieu1, lieu2):
    return distance.distance(lieu1, lieu2)

get_distance_udf = udf(get_distance)

def compute_distance(df):
    df = find_prefecture(df)
    df = df.withColumn("coordonnees_gps_tmp", to_tuple_udf(col("coordonnees_gps")))\
            .withColumn("gps_prefecture_tmp", to_tuple_udf(col("gps_prefecture")))
    print(df.show())
    #df = df.withColumn("distance", get_distance_udf(col("coordonnees_gps_tmp"), 
    #                                                col("gps_prefecture_tmp")))
    return df
