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

object AllLanguageMapper{

  def main(args: Array[String]) {
    val langID=List("en","de","es","fr","ja","nl")
    val whiteList=List("mappingbased_objects","mappingbased_objects_disjoint_domain",
    "mappingbased_objects_disjoint_range","mappingbased_objects_uncleaned","article_templates_nested","out_degree",
    "page_ids","page_length","page_links","persondata","pnd","redirects","revision_ids","revision_uris","transitive_redirects",
    "short_abstracts","skos_categories","specific_mappingbased_properties","template_parameters","topical_concepts")

    val sparkSession=SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("TripleIDMapper")
      .getOrCreate()

    //Can now use the previous code from Triple2IdMapper.scala
    for(lang<-langID){
      val savePath="/home/kathan/Downloads/datasets/".concat(lang).concat("/")
      val dataset:RDD[graph.Triple]=sparkSession.sparkContext.emptyRDD[graph.Triple]
      val existingFiles=scala.collection.mutable.MutableList[String]()
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
          existingFiles+=filename
          new URL(link1) #> new File(savePath.concat(filename).concat(".ttl.bz2")) !!
          val path="/home/kathan/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
          val fin = Files.newInputStream(Paths.get(path))
          val in = new BufferedInputStream(fin)
          val bzIn = new BZip2CompressorInputStream(in)
          val reader=new BufferedReader(new InputStreamReader(bzIn,Codec.UTF8.charSet))
          val a = Iterator.continually(reader.readLine()).takeWhile { r =>
            println(r)
            r!=null
          }.toSeq
          a.init.filterNot(_ == null) -> (a.last != null)
          var rdd=sparkSession.sparkContext.parallelize(a)
          rdd=rdd.filter(x=>x.charAt(0)=='<')
          val triplesRDD: RDD[graph.Triple] = rdd.map(line =>
            RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())

          dataset.union(triplesRDD)
        }
      }
      val dataset_for_one_language:TripleRDD=dataset
      //After this,we have a complete RDD(dataset) and can be used to generate EntityId and RelationId files using previous code.

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


      //Now to create the triple2id files for each dataset
      for(filename<-existingFiles){
        val path="/home/kathan/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_").concat(lang).concat(".ttl.bz2")
        val fin = Files.newInputStream(Paths.get(path))
        val in = new BufferedInputStream(fin)
        val bzIn = new BZip2CompressorInputStream(in)
        val reader=new BufferedReader(new InputStreamReader(bzIn,Codec.UTF8.charSet))
        val a = Iterator.continually(reader.readLine()).takeWhile { r =>
          println(r)
          r!=null
        }.toSeq
        a.init.filterNot(_ == null) -> (a.last != null)
        var rdd=sparkSession.sparkContext.parallelize(a)
        rdd=rdd.filter(x=>x.charAt(0)=='<')
        val triplesRDD = rdd.map(line =>
          RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
        val graph:TripleRDD=triplesRDD

        //Gets all triples with literals as objects and save as one text file.
        val triples_with_literals:TripleRDD=graph.filterObjects(_.isLiteral())
        //Remove those triples that have subject as blank nodes and object as literals.
        val triples_with_literals_modified=triples_with_literals.filterSubjects(_.isURI)

        //Gets all triples with subject as a URI.
        val subject_as_URI:TripleRDD=graph.filterSubjects(_.isURI())
        //From above, take onl those triples that have object as URI too!
        val s_o_URI=subject_as_URI.filterObjects(_.isURI())

        //Mappin <URI,URI,URI> files to ID's
        //First mapping the subject to ID's
        val s_o_URI_mapped=s_o_URI.map(x=>(x.getSubject,x.getPredicate,x.getObject))
        val subjectKeyTriples =s_o_URI_mapped.map(x =>(x._1,(x._2,x._3)))

        val subjectJoinedWithId: RDD[(Node, (Long, (Node, Node)))] =distinct_entities_withID.join(subjectKeyTriples)
        val subjectMapped=subjectJoinedWithId.map{
          case (oldSubject: Node, newTriple: (Long, (Node, Node))) =>
            (newTriple._1,newTriple._2._1,newTriple._2._2)
        }
        //Now mapping the objects to ID's
        val objectKeyTriples=subjectMapped.map(x=>(x._3,(x._1,x._2)))
        val objectJoinedWithId: RDD[(Node, (Long, (Long, Node)))] =distinct_entities_withID.join(objectKeyTriples)

        val objectMapped=objectJoinedWithId.map{
          case (oldObject: Node, newTriple: (Long, (Long, Node))) =>
            (newTriple._2._1,newTriple._2._2,newTriple._1)
        }
        //Now mapping the predicates to ID's
        val predicateKeyTriples=objectMapped.map(x=>(x._2,(x._1,x._3)))
        val predicateJoinedWithId: RDD[(Node, (Long, (Long, Long)))] =distinct_predicate_URI.join(predicateKeyTriples)

        val predicateMapped=predicateJoinedWithId.map{
          case (oldPredicate: Node, newTriple: (Long, (Long, Long))) =>
            (newTriple._2._1,newTriple._1,newTriple._2._2)
        }

        predicateMapped.coalesce(1,true).saveAsTextFile("/home/kathan/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_Uri2Id"))

        //    Now lets do it for triples with literals.
        val literals_mapped=triples_with_literals_modified.map(x=>(x.getSubject,x.getPredicate,x.getObject))
        val subjectAsKey=literals_mapped.map(x=>(x._1,(x._2,x._3)))
        val subjectID: RDD[(Node, (Long, (Node, Node)))] =distinct_entities_withID.join(subjectAsKey)

        val subjMap=subjectID.map{
          case (oldSubject: Node, newTriple: (Long, (Node, Node))) =>
            (newTriple._1,newTriple._2._1,newTriple._2._2)
        }

        val predicateAsKey=subjMap.map(x=>(x._2,(x._1,x._3)))
        val predicateID=distinct_predicate_URI.join(predicateAsKey)

        val predMap=predicateID.map{
          case (oldPredicate: Node, newTriple: (Long, (Long, Node))) =>
            (newTriple._2._1,newTriple._1,newTriple._2._2)
        }
        predMap.coalesce(1,true).saveAsTextFile("/home/kathan/Downloads/datasets/".concat(lang).concat("/").concat(filename).concat("_Literlas2Id"))
      }
    }
    //The code till now creates 2n+2 files for each language where n is number of files in each language directory.
    //The code from here merges all datasets across all languages and generates the required Id files.
    //Comment out the code that you dont need to use.But make sure if you run the code below,you have the datasets downloaded.
    //If we want to merge all datasets for each language,must follow this code.
    var datasetPath="/home/kathan/Downloads/datasets/"
    var mixedData:RDD[graph.Triple]=sparkSession.sparkContext.emptyRDD[graph.Triple]
    for(lang<-langID){
      for(dataset<-whiteList){
        val filePath=datasetPath.concat(lang).concat("/").concat(dataset).concat("_").concat(lang).concat(".ttl.bz2")
        val file=new File(filePath)
        if(file.isFile()){
          val inputStream: InputStream =Files.newInputStream(Paths.get(filePath))
          val fin=new BufferedInputStream(inputStream)
          val bzIn=new BZip2CompressorInputStream(fin)
          val reader=new BufferedReader(new InputStreamReader(bzIn,Codec.UTF8.charSet))
          val a=Iterator.continually(reader.readLine()).takeWhile{r=>
            r!=null
          }.toSeq
          a.init.filterNot(_ == null) -> (a.last != null)
          var rdd=sparkSession.sparkContext.parallelize(a)
          rdd=rdd.filter{x=>x.charAt(0)=='<'}
          val triplesRDD: RDD[graph.Triple] = rdd.map(line =>
            RDFDataMgr.createIteratorTriples(new ByteArrayInputStream(line.getBytes), Lang.NTRIPLES, null).next())
          mixedData=mixedData.union(triplesRDD)
        }
      }
    }
    val AllLangData:TripleRDD=mixedData
    //Generating ID's.
    //For subjects and objects
    val subjectURI=AllLangData.getSubjects.filter(_.isURI())
    val objectURI=AllLangData.getObjects.filter(_.isURI())
    val all_entities: RDD[Node] =subjectURI.union(objectURI).distinct()
    val distinct_entities_withID=all_entities.coalesce(1,true).zipWithUniqueId()
    distinct_entities_withID.coalesce(1,true).saveAsTextFile(datasetPath.concat("EntityId"))

    //For predicates
    val predicateURI=AllLangData.getPredicates.filter(_.isURI).distinct()
    val distinct_predicate_URI=predicateURI.coalesce(1,true).zipWithUniqueId()
    distinct_predicate_URI.coalesce(1,true).saveAsTextFile(datasetPath.concat("RelationId"))

    //Gets all triples with subject as a URI.
    val subject_as_URI:TripleRDD=AllLangData.filterSubjects(_.isURI())
    //From above, take onl those triples that have object as URI too!
    val s_o_URI=subject_as_URI.filterObjects(_.isURI())
    //Mappin <URI,URI,URI> files to ID's
    //First mapping the subject to ID's
    val s_o_URI_mapped=s_o_URI.map(x=>(x.getSubject,x.getPredicate,x.getObject))
    val subjectKeyTriples =s_o_URI_mapped.map(x =>(x._1,(x._2,x._3)))

    val subjectJoinedWithId: RDD[(Node, (Long, (Node, Node)))] =distinct_entities_withID.join(subjectKeyTriples)
    val subjectMapped=subjectJoinedWithId.map{
      case (oldSubject: Node, newTriple: (Long, (Node, Node))) =>
        (newTriple._1,newTriple._2._1,newTriple._2._2)
    }
    //Now mapping the objects to ID's
    val objectKeyTriples=subjectMapped.map(x=>(x._3,(x._1,x._2)))
    val objectJoinedWithId: RDD[(Node, (Long, (Long, Node)))] =distinct_entities_withID.join(objectKeyTriples)

    val objectMapped=objectJoinedWithId.map{
      case (oldObject: Node, newTriple: (Long, (Long, Node))) =>
        (newTriple._2._1,newTriple._2._2,newTriple._1)
    }
    //Now mapping the predicates to ID's
    val predicateKeyTriples=objectMapped.map(x=>(x._2,(x._1,x._3)))
    val predicateJoinedWithId: RDD[(Node, (Long, (Long, Long)))] =distinct_predicate_URI.join(predicateKeyTriples)

    val predicateMapped=predicateJoinedWithId.map{
      case (oldPredicate: Node, newTriple: (Long, (Long, Long))) =>
        (newTriple._2._1,newTriple._1,newTriple._2._2)
    }

    predicateMapped.coalesce(1,true).saveAsTextFile(datasetPath.concat("AllLangsUri2Id"))
  }
}

