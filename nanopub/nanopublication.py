from rdflib import Dataset, Namespace, Graph, ConjunctiveGraph, URIRef, Literal, RDF, XSD
import yaml
import os
import uuid
import datetime

class Nanopublication(Dataset):
    """
    A subclass of the rdflib Dataset class that comes pre-initialized with
    required Nanopublication graphs: np, pg, ag, pig, for nanopublication, provenance,
    assertion and publication info, respectively.

    """

    def __init__(self, name, base='http://example.org/'):
        """
        Initialize the graphs needed for the nanopublication
        """
        super(Dataset, self).__init__()

        self.name = name
        self.base = base

        # Virtuoso does not accept BNodes as graph names
        graph_uuid = str(uuid.uuid4())

        # We use 'Head' with capital for a better natural ordering of triples in the head
        head_graph_uri =  URIRef(self.base + name + '/Head/' + graph_uuid)
        self.default_context = Graph(store=self.store, identifier=head_graph_uri)

        # ----
        # The nanopublication graph
        # ----
        self.uri = URIRef(self.base + name + '/nanopublication/' + graph_uuid)


        # The Nanopublication consists of three graphs
        assertion_graph_uri = URIRef(self.base + name + '/assertion/' + graph_uuid)
        provenance_graph_uri = URIRef(self.base +  name + '/provenance/' + graph_uuid)
        pubinfo_graph_uri = URIRef(self.base + name + '/pubinfo/' + graph_uuid)

        self.ag = self.graph(assertion_graph_uri)
        self.pg = self.graph(provenance_graph_uri)
        self.pig = self.graph(pubinfo_graph_uri)

        # Namespace managing
        PROV = Namespace(URIRef("http://www.w3.org/ns/prov#"))
        NP = Namespace(URIRef("http://www.nanopub.org/nschema#"))

        self.default_context.bind('prov', PROV)
        self.default_context.bind('np', NP)

        # The nanopublication
        self.add((self.uri , RDF.type, NP['Nanopublication']))
        # The link to the assertion
        self.add((self.uri , NP['hasAssertion'], assertion_graph_uri))
        # The link to the provenance graph
        self.add((self.uri , NP['hasProvenance'], provenance_graph_uri))
        # The link to the publication info graph
        self.add((self.uri , NP['hasPublicationInfo'], pubinfo_graph_uri))

        # ----
        # The provenance graph
        # ----

        # Provenance information for the assertion graph (the data structure definition itself)
        # self.pg.add((assertion_graph_uri, PROV['wasDerivedFrom'], self.dataset_version_uri))
        # self.pg.add((dataset_uri, PROV['wasDerivedFrom'], self.dataset_version_uri))
        # self.pg.add((assertion_graph_uri, PROV['generatedAtTime'],
                     # Literal(timestamp, datatype=XSD.dateTime)))

        # ----
        # The publication info graph
        # ----

        # The URI of the latest version of this converter
        # TODO: should point to the actual latest commit of this converter.
        # TODO: consider linking to this as the plan of some activity, rather than an activity itself.
        agent_uri = URIRef('https://github.com/albertmeronyo/python-nanopub')

        timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
        self.pig.add((self.uri, PROV['wasGeneratedBy'], agent_uri))
        self.pig.add((self.uri, PROV['generatedAtTime'],
                      Literal(timestamp, datatype=XSD.dateTime)))


    def ingest(self, graph, target_graph=None):
        """
        Adds all triples in the RDFLib ``graph`` to this :class:`Nanopublication` dataset.
        If ``target_graph`` is ``None``, then the triples are added to the default graph,
        otherwise they are added to the indicated graph
        """
        if target_graph is None:
            for s, p, o in graph:
                self.add((s, p, o))
        else:
            for s, p, o in graph:
                self.add((s, p, o, target_graph))

    def as_string(self, output_format='trig'):
        serialized = bytes()
        x = ConjunctiveGraph()
        for s,p,o,g in self.quads((None,None,None,self.default_context.identifier)):
            x.add((s,p,o,self.default_context.identifier))
        serialized += x.serialize(format=output_format)
        x = ConjunctiveGraph()
        for s,p,o,g in self.quads((None,None,None,self.ag.identifier)):
            x.add((s,p,o,self.ag.identifier))
        serialized += x.serialize(format=output_format)
        x = ConjunctiveGraph()
        for s,p,o,g in self.quads((None,None,None,self.pg.identifier)):
            x.add((s,p,o,self.pg.identifier))
        serialized += x.serialize(format=output_format)
        x = ConjunctiveGraph()
        for s,p,o,g in self.quads((None,None,None,self.pig.identifier)):
             x.add((s,p,o,self.pig.identifier))
        serialized += x.serialize(format=output_format)

        return serialized

if __name__ == "__main__":
    np = Nanopublication('foo')
    print(np.serialize(format='trig').decode('utf-8'))
    exit(0)
