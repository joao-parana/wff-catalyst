
// Este script é para rodar no Ammonite.
// Dentro da shell REPL do Ammonitem, você deve invocar assim:
//   import $file.AddBarrierForConstrainedActivity, AddBarrierForConstrainedActivity._
// 
// Para testar com a versão 2.4.0 (não existe release disponível no momento), é necessário 
// fazer o build do Spark Localmente e usar:
/*
import coursier.MavenRepository
interp.repositories() ++= Seq(MavenRepository("file:/Users/admin/.m2/repository"))
import $ivy.`org.apache.spark::spark-sql:2.4.0`
*/

import $ivy.`org.apache.spark::spark-sql:2.3.0`

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.plans.logical.{ AnalysisBarrier, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.{Dataset, SparkSession}
import scala.collection.Seq
import scala.reflect.api.materializeTypeTag

import org.apache.spark.sql.catalyst.expressions.{NamedExpression, Expression}

object Provenance {
  def checkUDFForAnnotation(e: NamedExpression) = {
    // TODO: put here the correct implementation
    true
  }

  def checkUDFForAnnotation(e: Expression) = {
    // TODO: put here the correct implementation
    true
  }
}

// Creating a case class (schema of dataset)
case class R0(
               x: Int,
               p: Option[Int] = Some((new scala.util.Random).nextInt(999)),
               q: Option[Int] = Some((new scala.util.Random).nextInt(999)))

case class R1(
               x: Int,
               p: Int,
               q: Int,
               udfA_M3G: Double)


object WfFDataset {
  val oneMillion = 1000000
  // builder for Dataset
  def createDsR0(spark: SparkSession): Dataset[R0] = {
    import spark.implicits._
    val ds = spark.range(oneMillion) // can be more depending on memory available
    val _dsR0 = ds.map((i) => {
      R0(i.intValue() + 1)
    })
    // IMPORTANT: The cache here is mandatory
    _dsR0.cache()
  }
}

case class AddBarrierForConstrainedActivity(spark: SparkSession) extends Rule[LogicalPlan] {
  var prefix = ""
  def hasTwoUDFOneInEachProject(p: Project, pp: Project): Boolean = {
    // Two Adjacents Projects with or without AnalysisBarrier between them.
    val pFields: Seq[NamedExpression] = p.projectList
    val ppFields: Seq[NamedExpression] = pp.projectList
    val pCondition = pFields.map((e) => {
      e.toString.contains("UDF") && Provenance.checkUDFForAnnotation(e)
    }).filter((e) => {
      e
    })

    val ppCondition = ppFields.map((e) => { e.toString.contains("UDF") }).filter((e) => e)
    if (pCondition.isEmpty || ppCondition.isEmpty) {
      false
    } else {
      // a have only one UDF on project field list so I can do this:
      pCondition.head && ppCondition.head
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    var caseMatched = 0
    prefix = "•••• '" + spark.conf.get("br.cefet-rj.wff.workflow") + "' optimization: 01\n\n"
    val optimized = plan transform {
      case p @ Project(_, ab @ AnalysisBarrier(pp @ Project(_, grandGChild))) if hasTwoUDFOneInEachProject(p, pp) => {
        caseMatched = 1
        // Only one project is important
        val copied = p.copy(child = grandGChild)
        val theFields: Seq[NamedExpression] = copied.projectList
        // Proof of Concept only, so I can use hard-coded solution
        val modifiedFields = Seq(theFields(0), theFields(1), theFields(2), theFields(4), pp.projectList(3))
        copied.copy(projectList = modifiedFields)
      }
      case p @ Project(_, pp @ Project(_, grandChild)) if hasTwoUDFOneInEachProject(p, pp) => {
        caseMatched = 1
        // Only one project is important
        val copied = p.copy(child = grandChild)
        val theFields: Seq[NamedExpression] = copied.projectList
        // Proof of Concept only, so I can use hard-coded solution
        val modifiedFields = Seq(theFields(0), theFields(1), theFields(2), theFields(4), pp.projectList(3))
        copied.copy(projectList = modifiedFields)
      }
    }
    if (caseMatched > 0) {
      println(prefix + "\n•••• ChangePositionOfTwoAdjacentsProjectsContainingUDF invoked.\ncaseMatched: " + caseMatched)
    }
    optimized
  }
}

object ConstrainedActivity  {
  def main(args: Array[String])  {
    println("•••• ConstrainedActivity")

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    type ExtensionSetup = SparkSessionExtensions => Unit

    val f1: ExtensionSetup =
      e => e.injectResolutionRule(AddBarrierForConstrainedActivity)

    // Creating a SparkSession passing in the extension to inject the ResolutionRule
    val spark = SparkSession.builder().withExtensions(f1).master("local[3]").
      config("br.cefet-rj.wff.workflow", "w01").getOrCreate()

    // Here I start computation
    val dsR0 = WfFDataset.createDsR0(spark)
    val udf_A_M3G = (x: Int, p: Int) => {
      if (x < 6) println("udfA_M3G executed. p = " + p)
      Math.cos(p * p)
    }
    val udf_B_M3G = (x: Int, q: Int) => {
      if (x < 6) println("udfB_M3G executed. q = " + q)
      q + 1
    }

    println("*** I' going to register my UDF ***")
    val myUdfA_M3G = spark.udf.register("myUdfA", udf_A_M3G)
    val myUdfB_M3G = spark.udf.register("myUdfB", udf_B_M3G)

    val dsR1 = {
      dsR0.selectExpr("x", "p", "q", "myUdfA(x, p) as udfA_M3G")
    }

    val dsR2 = {
      import spark.implicits._
      val r = dsR1.collect().map((t) => {
        R1(t.getInt(0), t.getInt(1), t.getInt(2), t.getDouble(3))
      })

      val ds = spark.sparkContext.parallelize(r)
      ds.toDS.selectExpr("x", "p", "q", "udfA_M3G", "myUdfB(x, q) as udfB_M3G")

//      spark.sparkContext.parallelize(dsR1.collect().map((t) => {
//        R1(t.getInt(0), t.getInt(1), t.getInt(2), t.getDouble(3))
//      })).toDS.selectExpr("x", "p", "q", "udfA_M3G", "myUdfB(x, q) as udfB_M3G")
    }

    dsR2.show(5)

    // Let us get the execution plan for the query
    //println("••• \n")
    //println("logical: \n" + dsR2.queryExecution.logical)
    //println("analyzed: \n" + dsR2.queryExecution.analyzed)
    //println("withCachedData: \n" + dsR2.queryExecution.withCachedData)
    //println("optimized: \n" + dsR2.queryExecution.optimizedPlan)
    //
    println("\n••• explain(true):\n")
    dsR2.explain(true)
    //Logger.getLogger("org").setLevel(Level.DEBUG)
    //dsR2.show(10)

    // println("R0.selectExpr(\"x\", \"p\", \"q\", \"myUdfA(x, p) as udfA_M3G\").selectExpr(\"x\", \"p\", \"q\", \"udfA_M3G\", \"myUdfB(x, q) as udfB_M3G\")\n" +
    //   "Where R0 is the input Relation.")

    // Stop the underlying SparkContext in this session and clear out
    // the active and default session

    // spark.stop()
    // SparkSession.clearActiveSession()
    // SparkSession.clearDefaultSession()
    //
    // exit
  }
}

