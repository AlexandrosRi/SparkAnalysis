import java.io.{File, FileInputStream, StringReader}

import scala.io.Source
import scala.collection.JavaConverters._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark._
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
    //val localInputPath1 = "file:///home/hduser/input"
    //val localInputPath2 = "file:///home/hduser/input,file:///home/hduser/input2"
    //val localInputPathLG = "file:///home/hduser/inputMassive"
    val outputpathSeq = hdfspath + "outputSeq"
    val outputpathTxt = hdfspath + "outputTxt"
    val conf = new SparkConf().setAppName("Spark Analysis")
    val sc = new SparkContext(conf)

    //val inputdata = sc.wholeTextFiles(inputpath)
    //val inputdata = sc.wholeTextFiles(localInputPathLG)
    


    def getContext(dir: String): List[(String,String)] = {
      val files = getListOfFiles(dir)
      //println("I reached here")
      val fnc:List[(String,String)] = files.map(x => (x.getName, Source.fromFile(x).mkString + "\n"))
      
      return fnc
    }

    def getListOfFiles(dir: String):List[File] = {
      val d = new File(dir)
      if (d.exists && d.isDirectory) {
        d.listFiles.filter(_.isFile).toList
      } else {
        List[File]()
      }
    }

    def getMethodsString(javaContent:String)= {

      val in: StringReader = new StringReader(javaContent)
      var methodsString = List("")

      try {

        val cu: CompilationUnit = JavaParser.parse(in)

        methodsString = getMethods(cu);
      }
      finally {
        in.close()
      }
      //    val methString = methodsString
      methodsString
    }


    def getMethods(cu: CompilationUnit) = {

      val types: List[TypeDeclaration] = cu.getTypes.asScala.toList

      var methodsString = ""
      for (astType: TypeDeclaration <- types) {
        val members = astType.getMembers.asScala.toList
        for (member <- members) {
          member match {
            case method: FieldDeclaration =>
              methodsString += method.getType.toString + ":" + method.getVariables.get(0).getId.toString + ", "
            case _ =>
          }
        }
      }

      methodsString.stripSuffix(", ").split(", ").toList
    }

    /*
        val methodData = inputdata.map(x => (x._1, getMethodsString(x._1, x._2)))
        methodData.saveAsTextFile(outputpath)
         */

        val inputdata = getContext("/home/hduser/dpl/input")
        val methodData = inputdata.map(x => (x._1.hashCode.toLong, (x._1, getMethodsString(x._2))))
        val data = sc.parallelize(methodData)

        val classesWithAttributes: RDD[(VertexId, (String, List[String]))] = data
      //  val classConections = EdgeRDD.fromEdges()
        classesWithAttributes.saveAsTextFile(outputpathTxt)

//with a tag
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
