package net.sansa_stack.examples.spark.rdf

import java.net.{URI => JavaURI}

import net.sansa_stack.rdf.spark.io.NTripleReader
import net.sansa_stack.rdf.spark.model.{JenaSparkRDDOps, TripleRDD}
import org.apache.jena.graph.{Node, Node_URI}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object Triple2IdMapper{

  def main(args: Array[String])={
    val input="sansa-examples-spark/src/main/resources/*.ttl"

    val sparkSession=SparkSession.builder
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("Triple to ID Mapper ("+input+")")
      .getOrCreate()

    val ops=JenaSparkRDDOps(sparkSession.sparkContext)
    import ops._

    val triplesRDD=NTripleReader.load(sparkSession,JavaURI.create(input))

    val graph: TripleRDD=triplesRDD

    //Gets all triples with literals as objects and save as one text file.
    val triples_with_literals:TripleRDD=graph.filterObjects(_.isLiteral())
    //Remove those triples that have subject as blank nodes and object as literals.
    val triples_with_literals_modified=triples_with_literals.filterSubjects(_.isURI)

//    //Gets all triples with blank nodes as subjects.
//    val triples_with_blankNodes_subject=graph.filterSubjects(_.isBlank())
//    //Gets all triples with blank nodes as objects.
//    val triples_with_blankNodes_objects=graph.filterObjects(_.isBlank())
//    //Merges the above two RDD's.
//    val triples_with_blankNodes_mixed=triples_with_blankNodes_subject.union(triples_with_blankNodes_objects)
//    //Some overlap between the two files so remove repeating triples.
//    val distinct_triples_with_blankNodes=triples_with_blankNodes_mixed.distinct()

    //Gets all triples with subject as a URI.
    val subject_as_URI:TripleRDD=graph.filterSubjects(_.isURI())
    //From above, take onl those triples that have object as URI too!
    val s_o_URI=subject_as_URI.filterObjects(_.isURI())

    //Save all three of above datasets
//    triples_with_literals_modified.coalesce(1,true).saveAsTextFile("Literals")
//    distinct_triples_with_blankNodes.coalesce(1,true).saveAsTextFile("Blank Nodes")
//    s_o_URI.coalesce(1,true).saveAsTextFile("Triples with URI")


    //Generating ID's.
    //For subjects and objects
    val subjectURI=graph.getSubjects.filter(_.isURI())
    val objectURI=graph.getObjects.filter(_.isURI())
    val all_entities: RDD[Node] =subjectURI.union(objectURI).distinct()
    val distinct_entities_withID=all_entities.coalesce(1,true).zipWithUniqueId()
    distinct_entities_withID.coalesce(1,true).saveAsTextFile("EntityId")

    //For predicates
    val predicateURI=graph.getPredicates.filter(_.isURI).distinct()
    val distinct_predicate_URI=predicateURI.coalesce(1,true).zipWithUniqueId()
    distinct_predicate_URI.coalesce(1,true).saveAsTextFile("RelationId")

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

    predicateMapped.coalesce(1,true).saveAsTextFile("<URI,URI,URI>Mappings")
//    Till now we convereted the file which had triples as <URI,URI,URI>.

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

    predMap.coalesce(1,true).saveAsTextFile("LiteralsMapping")
  }
}