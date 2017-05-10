taxiText = sc.textFile(“path to csv”)

taxiSchema = StructType([StructField(“trip_distance”, StringType(), False),StructField(“pickup_longitude”, StringType(), False),StructField(“pickup_latitude”, IntegerType(), False),StructField(“fare_amount”, IntegerType(), False),StructField(“passenger_count”, StringType(), False)])

taxi = taxiText.map(lambda s: s.split(“,”)).filter(lambda s: s[0] != “”).map(lambda s:(str(s[0]), str(s[1]), int(s[2]), int(s[3]), str(s[6]) ))

taxidf = sqlContext.createDataFrame(taxi,taxiSchema)

taxidf.registerTempTable(“taxi”)

%%sql
SELECT * FROM taxi
