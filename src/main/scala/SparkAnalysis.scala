import java.io.{File, StringReader}
import scala.io.Source
import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import com.github.javaparser.ast.body._
import com.github.javaparser.ast.expr.{FieldAccessExpr, MethodCallExpr, NameExpr, StringLiteralExpr}
import com.github.javaparser.ast.stmt.{BlockStmt, ExpressionStmt}
import com.github.javaparser.ast.CompilationUnit
import com.github.javaparser.{ASTHelper, InstanceJavaParser, JavaParser, StringProvider}


object SparkAnalysis {

  def main(args: Array[String]) {
    val hdfspath:String = "hdfs://localhost:9000/user/hduser/"
    val outputpathSeq = hdfspath + "outputSeq"
    val outputpathTxt = hdfspath + "outputTxt"
    val conf = new SparkConf().setAppName("Spark Analysis")
    val sc = new SparkContext(conf)

    /*  Directory parsing helper methods  */
    def getContext(dir: String): List[(String,String)] = {
      val files = getListOfFiles(dir)
      val fnc:List[(String,String)] = files.map(x => (x.getName.stripSuffix(".java"), Source.fromFile(x).mkString + "\n"))
      fnc
    }

    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    /* FIELDS: helper methods and case class  */
    def getMethodsString(x1: String, javaContent:String) = {

      val in: StringReader = new StringReader(javaContent)
      var methodsString = List("")

      try {
        val cu: CompilationUnit = JavaParser.parse(in)
        methodsString = getMethods(x1, cu)
      }
      finally {
        in.close()
      }
      methodsString
    }


    def getMethods(x1: String, cu: CompilationUnit) = {

      val types: List[TypeDeclaration] = cu.getTypes.asScala.toList
      var methodsString = ""
      var flag = false

      for (astType: TypeDeclaration <- types) {
        val members = astType.getMembers.asScala.toList
        for (member <- members) {
          member match {
            case method: FieldDeclaration =>
              methodsString += method.getType.toString + ":" + method.getVariables.get(0).getId.toString + "----"
              flag = true
            case _ =>
          }
        }
      }

      flag match{
        case false =>
          List("none:n")
        case true =>
          methodsString.stripSuffix("----").split("----").toList
      }
    }

    def classHash(name: String): VertexId = {
      name.toLowerCase.replace(" ", "").hashCode.toLong
    }

    val inputLG = "/home/hduser/input2"
    val inputSM = "/home/hduser/dpl/input"

    val inputdata = sc.parallelize(getContext(inputSM))

    /*
      Refactor to make easier filling with only one parsing
      case class ClassData(fqName: String, attrs: List[(String, String)], ext: String)
    */

    case class ClassData(fqName: String, attrs: List[(String, String)])
    val classes = inputdata.map(x => ClassData(x._1, getMethodsString(x._1, x._2).map(a => {
       (a.split(":")(0), a.split(":")(1))
    }))) //      Refactor to make easier filling with only one parsing

    /* FIELDS: vertices | edges | graph  */
    val vertices = classes.map(x => (classHash(x.fqName), (x.fqName, x.attrs)))

    val edges: RDD[Edge[String]] = classes.flatMap { x =>
      val srcVid = classHash(x.fqName)
      x.attrs.map { field =>
        val dstVid = classHash(field._1)
        Edge(srcVid, dstVid, "has field")
      }
    }

    val defaultClass = ("Java Native", List(("int","a"), ("String","b")))
    val graph = Graph(vertices, edges, defaultClass)

//    println(graph.edges.collect.mkString("\n"))


    /* INHERITANCE: helper methods and case class  */
    def getExt(javaContent: String): String = {
      val in: StringReader = new StringReader(javaContent)
      var ext: String = ""

      try {

        val cu: CompilationUnit = JavaParser.parse(in)

        val types: List[TypeDeclaration] = cu.getTypes.asScala.toList

        for (astType: TypeDeclaration <- types) {
          astType match {
            case astType: ClassOrInterfaceDeclaration =>
              println(astType.getName + ":" + classHash(astType.getName)+ " extends " + astType.getExtends.toString)
              ext = astType.getExtends.toString.stripSuffix("]").stripPrefix("[")
            case _ =>
          }
        }
      }
      finally {
        in.close()
      }
      ext
    }
    case class ClassDataExt(fqName: String, ext: String)
    val classesExt = inputdata.map(x => ClassDataExt(x._1, getExt(x._2)))

    /* INHERITANCE: vertices | edges | graph  */
    val verticesExt: RDD[(VertexId, (String, String))] = classesExt.map(x => (classHash(x.fqName),(x.fqName, x.ext)))

    val edgesExt: RDD[Edge[String]] = classesExt.map { x =>
      val srcVidExt = classHash(x.fqName)
      val dstVidExt = classHash(x.ext)
      Edge(srcVidExt, dstVidExt, "extends")
      }

    val defaultClassExt = ("ext","ext")
    val graphExt = Graph(verticesExt, edgesExt, defaultClassExt)

    println(graphExt.edges.collect.mkString("\n"))
    println(graphExt.vertices.collect.mkString("\n"))
  }
}