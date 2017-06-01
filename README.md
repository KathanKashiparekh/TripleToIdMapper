# TripleToIdMapper
Takes in a datasets of the extension .ttl and generates unique ID's for distinct URI's in subject+object and predicate and maps the triples to an ID format.

In order to make it work following things should be kept in mind:
1) The SANSA-Examples repository must be cloned,built and the TripleOps.scala example working.
2) All dataset(.ttl) files should be in the "sansa-examples-spark/src/main/resources/" directory. 

The Triple2IdMapper.scala basically does the following jobs:
1) Reads in all .ttl files into one RDD.
2) Extracts triples with Literals as objects and stores it in "triples_with_literals_modified" varaible and extracts triples with the format <URI,URI,URI> and stores it in "s_o_URI" variable.
3) Takes disticnt URI's from subject+object pair and assign them unique sequential ID's.Mapping stored in EntityID.txt.
4) Take distinct URI's from the predicate and assign them unique sequential ID's.Mapping stored in folder RelationID.txt.
5) Maps the <URI,URI,URI> triples to their corresponding ID's and stores the triples in the directory "<URI,URI,URI>Mappings".
6) Maps the triples with literals as ojects to their ID's keeping the literals as they are and stores it in the directory "LiteralsMapping".

In order to run it,just run the .scala file. (Keeping in mind the 2 points made above)
