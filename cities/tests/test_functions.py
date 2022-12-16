from cities.cities_functions import split_lat_long, get_dept, departement_udf

def test_split_lat_long(spark_session):
  #GIVEN creation d'un dataframe de test en utilisant la spark_session
  input_list = [("Paris", "48.856614, 2.3522219"), ("Lyon", "45.764043, 4.835659"), ("Melun", None)]
  input_df = spark_session.createDataFrame(input_list, ["commune", "gps"])
  
  #WHEN
  actual_df = split_lat_long(input_df, "gps")
  actual = list(map(lambda x: x.asDict(), actual_df.collect()))

  #THEN
  expected = [
    {"commune": "Paris", "latitude": 48.856614, "longitude": 2.3522219},
    {"commune": "Lyon", "latitude": 45.764043, "longitude": 4.835659},
    {"commune": "Melun", "latitude": None, "longitude": None}
  ]
  assert actual == expected

def test_get_dept(spark_session):
  #GIVEN creation d'un dataframe de test en utilisant la spark_session
  input_list = [("Paris", "75000"), ("Lyon", "69000"), ("Melun", None)]
  input_df = spark_session.createDataFrame(input_list, ["commune", "code_postal"])
  
  #WHEN
  actual_df = get_dept(input_df, "code_postal")
  actual = list(map(lambda x: x.asDict(), actual_df.collect()))

  #THEN
  expected = [
    {"commune": "Paris", "dept": "75"},
    {"commune": "Lyon", "dept": "69"},
    {"commune": "Melun", "dept": None},
  ]
  assert actual == expected

def test_departement_udf(spark_session):
  #GIVEN creation d'un dataframe de test en utilisant la spark_session
  input_list = [("Paris", "75000"), ("Lyon", "69000"), ("Melun", None), ("Ajaccio", "20090")]
  input_df = spark_session.createDataFrame(input_list, ["commune", "code_postal"])
  
  #WHEN
  actual_df = departement_udf(input_df, "code_postal")
  actual = list(map(lambda x: x.asDict(), actual_df.collect()))

  #THEN
  expected = [
    {"commune": "Paris", "dept": "75"},
    {"commune": "Lyon", "dept": "69"},
    {"commune": "Melun", "dept": None},
    {"commune": "Ajaccio", "dept": "2A"},
  ]
  assert actual == expected
  