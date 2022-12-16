from pyspark.sql.functions import col, split, substring
from pyspark.sql.types import DoubleType

def split_lat_long(df, lat_long_col):
    df = df.withColumn("latitude", split(col(lat_long_col), ",").getItem(0).cast(DoubleType()))\
            .withColumn("longitude", split(col(lat_long_col), ",").getItem(1).cast(DoubleType()))\
            .drop(lat_long_col)
    return df

def get_dept(df, cp):
    return df.withColumn("dept", substring(cp, 1, 2)).drop(cp)
