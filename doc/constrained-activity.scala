case class R1(
  x: Int,
  p: Int,
  q: Int,
  udfA_M3G: Double
)

val tmpCollection = inputRelation.collect().map((t) => {
  R1(t.getInt(0), t.getInt(1), t.getInt(2), t.getDouble(3))
})

val outputRelation = spark.sparkContext.parallelize(tmpCollection).toDS
