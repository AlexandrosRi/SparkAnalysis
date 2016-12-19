import java.io.{File, StringReader}

import scala.io.Source
import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD

import com.github.javaparser.ast.body._
import com.github.javaparser.ast.expr.{FieldAccessExpr, MethodCallExpr, NameExpr, StringLiteralExpr}
import com.github.javaparser.ast.stmt.{BlockStmt, ExpressionStmt}
import com.github.javaparser.ast.CompilationUnit
import com.github.javaparser.{ASTHelper, InstanceJavaParser, JavaParser, StringProvider}


object SparkAnalysis {

  def main(args: Array[String]) {
    //test
    val hdfspath:String = "hdfs://localhost:9000/user/hduser/"
    //val inputpath = hdfspath + "input"
    val outputpathSeq = hdfspath + "outputSeq"
    val outputpathTxt = hdfspath + "outputTxt"
    val conf = new SparkConf().setAppName("Spark Analysis")
    val sc = new SparkContext(conf)





/*
    val inputdata = getContext("/home/hduser/dpl/input")
    val methodData = inputdata.map(x => (x._1.hashCode.toLong, (x._1, getMethodsString(x._2))))
    val data = sc.parallelize(methodData)
        val classesWithAttributes: RDD[(VertexId, (String, List[String]))] = data
*/




    def getContext(dir: String): List[(String,String)] = {
      val files = getListOfFiles(dir)
      //println("I reached here")
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

    def getMethodsString(x1: String, javaContent:String)= {

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
   //       println("****" + x1)
          methodsString.stripSuffix("----").split("----").toList
      }
    }

    def classHash(name: String): VertexId = {
      name.toLowerCase.replace(" ", "").hashCode.toLong
    }


    /*
        val methodData = inputdata.map(x => (x._1, getMethodsString(x._1, x._2)))
        methodData.saveAsTextFile(outputpath)
         */

    val inputLG = "/home/hduser/input2"
    val inputSM = "/home/hduser/dpl/input"

    val inputdata = sc.parallelize(getContext(inputLG))

    case class ClassData(fqName: String, attrs: List[(String, String)])
    val classes = inputdata.map(x => ClassData(x._1, getMethodsString(x._1, x._2).map(a => {
      println(x._1)
      (a.split(":")(0), a.split(":")(1))
    })))

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


    println(graph.vertices.collect.mkString("\n"))


   }

}


/* def getMethodsString(fName:String, javaContent:String): String = {
//def getMethodsString(javaContent:String): String = {
  val in: StringReader = new StringReader(javaContent)
  var methodsString = " "

  try {
    //println("Attempting to parse: "+fName)
    val cu: CompilationUnit = JavaParser.parse(in)

    methodsString = getMethods(cu);

  }
  finally {
    in.close()
  }
  return methodsString
}

def getMethods(cu: CompilationUnit): String ={

  val types:List[TypeDeclaration] = cu.getTypes.asScala.toList
  var methodsString = " "
  for (astType:TypeDeclaration <- types) {
    val members = astType.getMembers.asScala.toList
    for (member <- members) {
      member match {
        //case method: MethodDeclaration =>
          //methodsString += method.getDeclarationAsString + ", "
        case method: FieldDeclaration =>
          methodsString += method.getType.toString  + ":" + method.getVariables.get(0).getId.toString + ", "
        case _ =>
      }
    }
  }
  return methodsString
}*/
