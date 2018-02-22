// Este script é para rodar no Ammonite.
// Crie o arquivo catalyst_04.sc com este conteúdo
// Dentro da shell REPL do Ammonitem, você deve invocar assim:
//   import $file.map_pushdown, map_pushdown._
// 
// Mas antes execute o comandos abaixo
// import $ivy.`org.apache.spark::spark-sql:2.3.0`
// 
// Para testar com a versão 2.3.0 (só existe beta disponível no momento), é necessário 
// fazer o build do Spark Localmente e usar:
//
// import coursier.MavenRepository
// interp.repositories() ++= Seq(MavenRepository("file:/Users/admin/.m2/repository"))
// import $ivy.`org.apache.spark::spark-sql:2.3.0`

import org.apache.spark.sql.SparkSessionExtensions
// AnalysisBarrier incluido na versão 2.3.0
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisBarrier, Filter, LogicalPlan, Project, Sort}
// import org.apache.spark.sql.catalyst.plans.logical.AnalysisBarrier
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions.NamedExpression


import org.apache.spark.sql.functions._
// import org.apache.spark.sql.catalyst._
// wffVersion()

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

def setupLog4J(): Unit = {
  val listDEBUGDep = "sql.UDFRegistration" :: "sql.catalyst" :: Nil // sql.catalyst.parser
  listDEBUGDep.foreach((suffix) => {
    Logger.getLogger("org.apache.spark." + suffix).setLevel(Level.WARN)
  })
  val listINFODep = "storage.ShuffleBlockFetcherIterator" ::
    "storage.memory.MemoryStore" :: "scheduler.TaskSetManager" :: "scheduler.TaskSchedulerImpl" :: "scheduler.DAGScheduler" ::
    "ContextCleaner" :: Nil // "executor.Executor"::
  listINFODep.foreach((suffix) => {
    Logger.getLogger("org.apache.spark." + suffix).setLevel(Level.WARN)
  })
}

// setupLog4J()

case class ChangePositionOfTwoAdjacentsProjectsContainingUDF(spark: SparkSession) extends Rule[LogicalPlan] {
  var prefix = ""
  def hasTwoUDFOneInEachProject(p: Project, pp: Project): Boolean = {
    // veio dois Project consecutivos
    // ••• -> p  = Project [x#8, p#9, q#10, udfA_99#27, UDF:myUdfB(p#9) AS udfB_10#114]
    // ••• -> pp = Project [x#8, p#9, q#10, if (isnull(p#9)) null else UDF:myUdfA(p#9) AS udfA_99#27]

    val pFields: Seq[NamedExpression] = p.projectList
    val ppFields : Seq[NamedExpression] = pp.projectList
    val pCondition = pFields.map((e) => {
      println("\n••• " +  e + " ======> " + e.toString.contains("UDF"))
      e.toString.contains("UDF")
    }).filter((e) => {
      e
    })

    val ppCondition = ppFields.map((e) => {
      println("\n••• " +  e + " ======> " + e.toString.contains("UDF"))
      e.toString.contains("UDF")}).filter((e) => e)

    println(prefix + " ••••\n••• -> p  = " + p)
    println(prefix + " ••••\n••• -> pp = " + pp)
    println(prefix + " ••••\n••• -> pCondition.size  = " + pCondition.size)
    println(prefix + " ••••\n••• -> ppCondition.size = " + ppCondition.size)

    // if (pCondition.size > 1 || ppCondition.size > 1) {
    //   println(prefix + " ••••\n••• ${pCondition.size}  ${ppCondition.size}.")
    //   throw new UnsupportedOperationException(s"UDF must be apear only one time but occur ${pCondition.size} | ${ppCondition.size}.")
    // } else {
      if (pCondition.isEmpty || ppCondition.isEmpty) {
        println("\n••• RETURN ========> " + false)
        false
      } else {
        Logger.getLogger("org").setLevel(Level.DEBUG)
        println("\n••• RETURN ========> " + (pCondition.head && ppCondition.head))
        // if a have only one UDF on project field list I can do this:
        pCondition.head && ppCondition.head
      }
    // }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    var caseMatched = 0
    prefix = "•••• br.cefet-rj.wff.workflow: '" + spark.conf.get("br.cefet-rj.wff.workflow") +
      "' optimization: 01\n\n"
    println(prefix + " ••••\n••• -> " + plan)
    val optimized = plan transform {
      case p @ Project(_, ab @ AnalysisBarrier( pp @ Project(_,  grandGChild)))
        if hasTwoUDFOneInEachProject(p, pp) => {
        caseMatched = 1
        // Only one project is important
        println(prefix + " ••••\n••• original = " + p + "\n")
        val copied  = p.copy(child = grandGChild)
        println(prefix + " ••••\n••• copied = " + copied + "\n")
        val theFields: Seq[NamedExpression] = copied.projectList
        val modifiedFields  = Seq(theFields(0), theFields(1),theFields(2),theFields(4), pp.projectList(3))
        val modified = copied.copy(projectList = modifiedFields)
        println(prefix + " ••••\n••• modified = " + modified + "\n")
        modified
      }
      case p @ Project(_, pp @ Project(_,  grandChild))
        if hasTwoUDFOneInEachProject(p, pp) => {
        caseMatched = 1
        // Only one project is important
        println(prefix + " ••••\n••• original = " + p + "\n")
        val copied  = p.copy(child = grandChild)
        println(prefix + " ••••\n••• copied = " + copied + "\n")
        val theFields: Seq[NamedExpression] = copied.projectList
        val modifiedFields  = Seq(theFields(0), theFields(1),theFields(2),theFields(4), pp.projectList(3))
        val modified = copied.copy(projectList = modifiedFields)
        println(prefix + " ••••\n••• modified = " + modified + "\n")
        modified
      }
    }
    if (caseMatched > 0) {
      println (prefix + "\n•••• ChangePositionOfTwoAdjacentsProjectsContainingUDF invoked.\ncaseMatched: " + caseMatched +
        "\nplan:\n" + plan + "\n•••• optimized:\n"+ optimized +
        "\n•••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••\n")
    }
    println("\n••••••••••••••••••••••••••••")
    optimized
  }
}

/**
  * Add a new Optimizer rule: ReorderColumnsOnProject 
  * The optimizer already collapse two adjacent Project operators
  */
case class ReorderColumnsOnProject(spark: SparkSession) extends Rule[LogicalPlan] {
  var prefix = ""

  def apply(plan: LogicalPlan): LogicalPlan = {
    var caseMatched = 0
    prefix = "•••• br.cefet-rj.wff.workflow: '" + spark.conf.get("br.cefet-rj.wff.workflow") +
      "' optimization: 02"
    println(prefix + " ••••\n••• -> " + plan)
    val optimized = plan transform {
      case p @ Project(fields, child) => {
        caseMatched = 2
        // val fields = p.projectList
        if (checkConditions(fields, p.child)) {
          val modifiedFieldsObject = getModifiedFields(fields)
          println (prefix + "\n•••• ReorderColumnsOnProject invoked.\ncaseMatched: " + caseMatched +
            "\nfieldsObject:        " + fields +
            "\nmodifiedFieldsObject:" + modifiedFieldsObject + "\n")
          val projectUpdated = p.copy(modifiedFieldsObject)
          projectUpdated
        } else {
          p
        }
      }
    }
    if (caseMatched > 0) {
      println (prefix + "\n•••• ReorderColumnsOnProject invoked.\ncaseMatched: " + caseMatched +
        "\nplan:\n" + plan + "\n•••• optimized:\n"+ optimized +
        "\n•••••••••••••••••••••••••••••••••••••••••••••••••••••••••••••\n")
    }
    println("\n••••••••••••••••••••••••••••")
    optimized
  }

  private def checkConditions(fields: Seq[NamedExpression], child: LogicalPlan): Boolean = {
    // compare UDFs computation cost and return Boolean
    val needsOptimization = listHaveTwoUDFsEnabledForOptimization(fields)
    if (needsOptimization) println(fields.mkString(" | "))
    needsOptimization
  }

  private def listHaveTwoUDFsEnabledForOptimization(fields: Seq[NamedExpression]): Boolean = {
    // a simple priority order based on UDF name suffix
    val myPriorityList = fields.map((e) => {
      if (e.name.toString().startsWith("udf")) {
        Integer.parseInt(e.name.toString().split("_")(1))
      } else {
        0
      }
    }).filter(e => e > 0)

    // Do UDF with less cost before, so I need change the fields order
    myPriorityList.size == 2 && myPriorityList(0) > myPriorityList(1)
    false
  }

  private def getModifiedFields(fields: Seq[NamedExpression]): Seq[NamedExpression] = {
    // change order on field list. Return LogicalPlan modified
    val myListWithUDF = fields.filter((e) =>  e.name.toString().startsWith("udf"))
    if (myListWithUDF.size != 2) {
      throw new UnsupportedOperationException(s"The size of UDF list have ${myListWithUDF.size} elements.")
    }
    val myModifiedList: Seq[NamedExpression] = Seq(myListWithUDF(1), myListWithUDF(0))
    val myListWithoutUDF = fields.filter((e) =>  !e.name.toString().startsWith("udf"))
    val modifiedFielsObject = getFieldsReordered(myListWithoutUDF, myModifiedList)
    val msg = "•••• optimizePlan called : " + fields.size + " columns on Project.\n" +
      "•••• fields: " + fields.mkString(" | ") + "\n" +
      "•••• UDFs to reorder:\n" + myListWithUDF.mkString(" | ") + "\n" +
      "•••• field list Without UDF: " + myListWithoutUDF.mkString(" | ") + "\n" +
      "•••• modifiedFielsObject: " + modifiedFielsObject.mkString(" | ") + "\n"
    // STR_TO_DEBUG = STR_TO_DEBUG + msg
    modifiedFielsObject
  }

  private def getFieldsReordered(fieldsWithoutUDFs: Seq[NamedExpression],
                                 fieldsWithUDFs: Seq[NamedExpression]): Seq[NamedExpression] = {
    fieldsWithoutUDFs.union(fieldsWithUDFs)
  }
}

Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
type ExtensionSetup = SparkSessionExtensions => Unit

val f1: ExtensionSetup =
  e =>  e.injectResolutionRule(ChangePositionOfTwoAdjacentsProjectsContainingUDF)


val f2: ExtensionSetup =
  e =>  e.injectOptimizerRule(ReorderColumnsOnProject)

// Create a SparkSession passing in the extensions, injecting the CollapseSorts
// optimizer rule

// br.cefet-rj.wff.opt8n
Logger.getLogger("org").setLevel(Level.ERROR)
val spark = SparkSession.builder().withExtensions(f1).withExtensions(f2).master("local[3]").
                        config("br.cefet-rj.wff.workflow", "w01").
                        config("br.cefet-rj.wff.opt8n", "02").getOrCreate()
Logger.getLogger("org").setLevel(Level.WARN)

case class R0(x: Int,
              p: Option[Int] = Some((new scala.util.Random).nextInt(999)),
              q: Option[Int] = Some((new scala.util.Random).nextInt(999))
             )

def createDsR0(spark: SparkSession): Dataset[R0] = {
  val ds = spark.range(3)
  import spark.implicits._
  val xdsR0 = ds.map((i) => {
    R0(i.intValue() + 1)
  })
  // IMPORTANT: The cache here is mandatory
  xdsR0.cache()
}

// Here I start computation

val dsR0 = createDsR0(spark)
val udfA_99 = (p: Int) => { println("udfA_99 executed. p = " + p) ; Math.cos(p * p) }  // higher cost Function
val udfB_10 = (q: Int) => { println("udfB_10 executed. q = " + q) ; q + 1 }            // lower cost Function

println("*** I' going to register my UDF ***")
val myUdfA_99 = spark.udf.register("myUdfA", udfA_99)
val myUdfB_10 = spark.udf.register("myUdfB", udfB_10)

val dsR1 = {
  val ret1DS = dsR0.selectExpr("x", "p", "q", "myUdfA(p) as udfA_99")
  // val result = ret1DS.cache()
  // dsR0.show()
  // result.show()
  // println("*** queryExecution.logical ***")
  // print(ret1DS.queryExecution.logical)

  // println("*** explain(true) ***")
  // ret1DS.explain(true)

  ret1DS
}

val dsR2 = {
  val ret2DS = dsR1.selectExpr("x", "p", "q", "udfA_99", "myUdfB(q) as udfB_10")
  // val result = ret2DS.cache()
  // dsR0.show()
  // dsR1.show()
  // result.show()
  // println("*** queryExecution.logical ***")
  // print(ret2DS.queryExecution.logical)

  // println("*** explain(true) ***")
  // ret2DS.explain(true)

  ret2DS
}


// Let us get the execution plan for the query
dsR2.explain(true)
dsR2.show

println("R0.selectExpr(\"x\", \"p\", \"q\", \"myUdfA(p) as udfA_99\").selectExpr(\"x\", \"p\", \"q\", \"udfA_99\", \"myUdfB(q) as udfB_10\")\n"+
  "Where R0 is the input Relation.")

// Stop the underlying SparkContext in this session and clear out the active and
// default session
spark.stop()
SparkSession.clearActiveSession()
SparkSession.clearDefaultSession()

exit
