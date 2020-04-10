qonfig = {
    endpoints: {
      "default": "http://$HOST/sparql/query",
    },
    prefixes: {
      "":       "urn:plain:",
      "rdf":    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
      "rdfs":   "http://www.w3.org/2000/01/rdf-schema#",
      "owl":    "http://www.w3.org/2002/07/owl#",
      "xsd":    "http://www.w3.org/2001/XMLSchema#",
      "foaf":   "http://xmlns.com/foaf/0.1/",
      "dct":    "http://purl.org/dc/terms/",
    },
    queries: [
      { "name": "Properties of a named bathing water",
        "query": "select ?predicate ?object\nwhere {\n" +
                 "  ?bw rdfs:label \"Spittal\"@en ;\n" +
                 "      ?predicate ?object\n}"
      },
      { "name": "all OWL classes",
        "query": "select ?class ?label ?description\nwhere {\n" +
                 "  ?class a owl:Class.\n" +
                 "  optional { ?class rdfs:label ?label}\n" +
                 "  optional { ?class rdfs:comment ?description}\n}"
      },
      {
        'name': 'Example with embedded comments',
        'query': '# comment 1\n@prefix foo: <http://fubar.com/foo>.\n@prefix bar: <http://fubar.com/bar>.\n#comment 2\nselect * {}'
      }
    ],
    allowQueriesFromURL: true
};