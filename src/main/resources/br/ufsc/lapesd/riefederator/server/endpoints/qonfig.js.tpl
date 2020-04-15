qonfig = {
    endpoints: {
      "default": "http://$HOST/sparql/query",
    },
    prefixes: {
      "":       "urn:plain:",
      "mod":    "https://alexishuf.bitbucket.io/dexa-2020/modalidades.ttl#",
      "rdf":    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
      "rdfs":   "http://www.w3.org/2000/01/rdf-schema#",
      "owl":    "http://www.w3.org/2002/07/owl#",
      "xsd":    "http://www.w3.org/2001/XMLSchema#",
      "foaf":   "http://xmlns.com/foaf/0.1/",
      "dct":    "http://purl.org/dc/terms/",
    },
    queries: [
      { "name": "All RDFS classes",
        "query": "select ?class ?label ?description\nwhere {\n" +
                 "  ?class a rdfs:Class.\n}"
      },
      { "name": "Procurements of contracts (slow)",
        "query": "SELECT ?id ?startDate ?openDate ?modDescr WHERE {\n" +
                 "    ?contract :id ?id ;\n" +
                 "              :unidadeGestora ?ug ;\n" +
                 "              :dimCompra/:numero ?numProc ;\n" +
                 "              :dataInicioVigencia ?startDate ;\n" +
                 "              :dataFimVigencia ?endDate ;\n" +
                 "              :modalidadeCompra/:descricao ?modDescr\n" +
                 "              FILTER(?startDate >= \"2019-12-01\"^^xsd:date)\n" +
                 "              FILTER(?endDate <= \"2020-12-02\"^^xsd:date) .\n" +
                 "    ?ug :codigo ?codUG ;\n" +
                 "        :orgaoVinculado/:codigoSIAFI \"26246\" .\n" +
                 "\n" +
                 "    ?descr mod:hasDescription ?modDescr ;\n" +
                 "           mod:hasCode ?modCode .\n" +
                 "\n" +
                 "    ?proc :id ?procId;\n" +
                 "          :unidadeGestora/:codigo ?codUG;\n" +
                 "          :licitacao/:numero ?numProc;\n" +
                 "          :modalidadeLicitacao/:codigo ?modCode;\n" +
                 "          :dataAbertura ?openDate .\n" +
                 "}\n"
      },
    ],
    allowQueriesFromURL: true
};