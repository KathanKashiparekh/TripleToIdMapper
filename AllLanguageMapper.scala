/**
  * Created by kathan on 1/6/17.
  */
package net.sansa_stack.examples.spark.rdf

import java.net.{HttpURLConnection, URL, URI => JavaURI}

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.jena.graph.{Node, Node_URI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.util.zip.{GZIPInputStream, GZIPOutputStream}
import java.io._

import scala.io.{Codec, Source}
import java.nio.charset.Charset

import org.apache.commons.validator.routines.UrlValidator
import org.apache.jena.graph
import org.apache.jena.riot.{Lang, RDFDataMgr}
import org.apache.spark.SparkContext

import scala.language.implicitConversions
//import java.io.{Reader,BufferedReader}
import org.apache.commons.compress.compressors.bzip2.{BZip2CompressorInputStream,BZip2CompressorOutputStream}
import java.io.{File, InputStream, OutputStream}
import java.io.BufferedInputStream
import java.nio.file.Files
import java.nio.file.Paths

import scala.util.Try
import scala.collection.mutable

import sys.process._

object RichReader
{
  implicit def wrapReader(reader: BufferedReader) = new RichReader(reader)

  implicit def wrapReader(reader: Reader) = new RichReader(reader)
}


class RichReader(reader: BufferedReader) {

  def this(reader: Reader) = this(new BufferedReader(reader))

  /**
    * Process all lines. The last value passed to proc will be null.
    */
  def foreach[U](proc: String => U): Unit = {
    while (true) {
      val line = reader.readLine()
      proc(line)
      if (line == null) return
    }
  }
}

/**
  * Allows common handling of java.io.File and java.nio.file.Path
  */
abstract class FileLike[T] {

  /**
    * @return full path
    */
  def toString: String

  /**
    * @return file name, or null if file path has no parts
    */
  def name: String

  def resolve(name: String): Try[T]

  def names: List[String]

  def list: List[T]

  def exists: Boolean

  @throws[java.io.IOException]("if file does not exist or cannot be deleted")
  def delete(recursive: Boolean = false): Unit

  def size(): Long

  def isFile: Boolean

  def isDirectory: Boolean

  def hasFiles: Boolean

  def inputStream(): InputStream

  def outputStream(append: Boolean = false): OutputStream

  def getFile: File
}

/**
  * TODO: modify the bzip code such that there are no run-time dependencies on commons-compress.
  * Users should be able to use .gz files without having commons-compress on the classpath.
  * Even better, look for several different bzip2 implementations on the classpath...
  */
object IOUtils {

  /**
    * Map from file suffix (without "." dot) to output stream wrapper
    */
  val zippers = Map[String, OutputStream => OutputStream] (
    "gz" -> { new GZIPOutputStream(_) },
    "bz2" -> { new BZip2CompressorOutputStream(_) }
  )

  /**
    * Map from file suffix (without "." dot) to input stream wrapper
    */
  val unzippers = Map[String, InputStream => InputStream] (
    "gz" -> { new GZIPInputStream(_) },
    "bz2" -> { new BZip2CompressorInputStream(_,true) }
  )

  /**
    * use opener on file, wrap in un/zipper stream if necessary
    */
  private def open[T](file: FileLike[_], opener: FileLike[_] => T, wrappers: Map[String, T => T]): T = {
    val name = file.name
    val suffix = name.substring(name.lastIndexOf('.') + 1)
    wrappers.getOrElse(suffix, identity[T] _)(opener(file))
  }

  /**
    * open output stream, wrap in zipper stream if file suffix indicates compressed file.
    */
  def outputStream(file: FileLike[_], append: Boolean = false): OutputStream =
    open(file, _.outputStream(append), zippers)

  /**
    * open input stream, wrap in unzipper stream if file suffix indicates compressed file.
    */
  def inputStream(file: FileLike[_]): InputStream =
    open(file, _.inputStream(), unzippers)

  /**
    * open output stream, wrap in zipper stream if file suffix indicates compressed file,
    * wrap in writer.
    */
  def writer(file: FileLike[_], append: Boolean = false, charset: Charset = Codec.UTF8.charSet): Writer =
    new OutputStreamWriter(outputStream(file, append), charset)

  /**
    * open input stream, wrap in unzipper stream if file suffix indicates compressed file,
    * wrap in reader.
    */
  def reader(file: FileLike[_], charset: Charset = Codec.UTF8.charSet): Reader =
    new InputStreamReader(inputStream(file), charset)

  /**
    * open input stream, wrap in unzipper stream if file suffix indicates compressed file,
    * wrap in reader, wrap in buffered reader, process all lines. The last value passed to
    * proc will be null.
    */
  def readLines(file: FileLike[_], charset: Charset = Codec.UTF8.charSet)(proc: String => Unit): Unit = {
    val reader: Reader = this.reader(file)

    import  RichReader._
    try {
      for (line<- reader) {
        proc(line)
      }
    }
    finally reader.close()
  }

  /**
    * Copy all bytes from input to output. Don't close any stream.
    */
  def copy(in: InputStream, out: OutputStream) : Unit = {
    val buf = new Array[Byte](1 << 20) // 1 MB
    while (true)
    {
      val read = in.read(buf)
      if (read == -1)
      {
        out.flush
        return
      }
      out.write(buf, 0, read)
    }
  }

}

object AllLanguageMapper{

  def main(args: Array[String]) {
    val langID=List("en","de","es","fr","ja","nl")
    val whiteList=List("infobox_properties")

    val sparkSession=SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("TripleIDMapper")
      .getOrCreate()

    //Can now use the previous code from Triple2IdMapper.scala
    for(lang<-langID){
      val dataset:RDD[graph.Triple]=sparkSession.sparkContext.emptyRDD[graph.Triple]
      val savePath="/home/kathan/Downloads/".concat(lang).concat("/")
      for(filename<-whiteList){
        val link="http://downloads.dbpedia.org/2016-04/core-i18n/"
        val link1=link.concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
        //1) Check if link is valid URL.
        //2) If it is,download it and do the above work and merge to the rdd.
        //3) If it isnt,use try catch or something.
        val connection=(new URL(link1)).openConnection.asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("HEAD")
        connection.connect()
        if(connection.getResponseCode()!=404){ //Can also have !=404
//          println("URL Exists")
          //Insert code here to download the file.
          new URL(link1) #> new File(savePath.concat(filename).concat(".ttl.bz2"))
          val path="/home/kathan/Downloads/".concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
          val fin = Files.newInputStream(Paths.get(path))
          val in = new BufferedInputStream(fin)
          val bzIn = new BZip2CompressorInputStream(in)
          val reader=new BufferedReader(new InputStreamReader(bzIn,Codec.UTF8.charSet))
          val a = Iterator.continually(reader.readLine()).takeWhile { r =>
            println(r)
            r!=null
          }.toSeq
          a.init.filterNot(_ == null) -> (a.last != null)
          val rdd=sparkSession.sparkContext.parallelize(a)
          val triplesRDD: RDD[graph.Triple] = rdd.map(line =>
            RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())

          dataset.union(triplesRDD)
        }
      }
      val dataset_for_one_language:TripleRDD=dataset
      //After this,we have a complete RDD(dataset) and can be used to generate EntityId and RelationId files using previous code.

      //Gets all triples with literals as objects.
      val triples_with_literals:TripleRDD=dataset_for_one_language.filterObjects(_.isLiteral())
      //Remove those triples that have subject as blank nodes and object as literals.
      val triples_with_literals_modified=triples_with_literals.filterSubjects(_.isURI)

      //Gets all triples with subject as a URI.
      val subject_as_URI:TripleRDD=dataset_for_one_language.filterSubjects(_.isURI())
      //From above, take onl those triples that have object as URI too!
      val s_o_URI=subject_as_URI.filterObjects(_.isURI())


      //Generating ID's.
      //For subjects and objects
      val subjectURI=dataset_for_one_language.getSubjects.filter(_.isURI())
      val objectURI=dataset_for_one_language.getObjects.filter(_.isURI())
      val all_entities: RDD[Node] =subjectURI.union(objectURI).distinct()
      val distinct_entities_withID=all_entities.coalesce(1,true).zipWithUniqueId()
      distinct_entities_withID.coalesce(1,true).saveAsTextFile(savePath.concat("EntityId"))

      //For predicates
      val predicateURI=dataset_for_one_language.getPredicates.filter(_.isURI).distinct()
      val distinct_predicate_URI=predicateURI.coalesce(1,true).zipWithUniqueId()
      distinct_predicate_URI.coalesce(1,true).saveAsTextFile(savePath.concat("RelationId"))
    }
  }
}

