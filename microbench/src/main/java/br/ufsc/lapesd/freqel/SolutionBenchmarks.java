/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package br.ufsc.lapesd.freqel;

import br.ufsc.lapesd.freqel.jena.query.JenaSolution;
import br.ufsc.lapesd.freqel.model.term.Term;
import br.ufsc.lapesd.freqel.model.term.URI;
import br.ufsc.lapesd.freqel.model.term.std.StdLit;
import br.ufsc.lapesd.freqel.model.term.std.StdURI;
import br.ufsc.lapesd.freqel.query.results.Solution;
import br.ufsc.lapesd.freqel.query.results.impl.ArraySolution;
import br.ufsc.lapesd.freqel.query.results.impl.MapSolution;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.*;
import org.apache.jena.vocabulary.XSD;
import org.openjdk.jmh.annotations.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static br.ufsc.lapesd.freqel.jena.JenaWrappers.fromJena;
import static java.util.Arrays.asList;
import static org.apache.jena.rdf.model.ResourceFactory.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class SolutionBenchmarks {
    private static final String EX = "http://example.org/ns#";
    private static final URI xsdInt = new StdURI(XSD.xint.getURI());
    private static final Property value = createProperty(EX+"value");
    private final int ROWS = 128;

    private QueryExecution jenaExecution;
    private List<QuerySolution> jenaSolutions;
    private JenaSolution.Factory jenaSolutionFactory;
    private List<ImmutablePair<Term, Term>> stdSolutions;
    private List<ImmutablePair<RDFNode, RDFNode>> jenaSolutionsTerms;
    private ArraySolution.ValueFactory arraySolutionFactory;

    @Setup(Level.Trial)
    public void setUp() {
        jenaSolutionsTerms = new ArrayList<>(ROWS);
        Model model = ModelFactory.createDefaultModel();
        for (int i = 0; i < ROWS; i++) {
            Resource uri = createResource(EX + i);
            Literal lit = createTypedLiteral(i);
            model.add(uri, value, lit);
            jenaSolutionsTerms.add(ImmutablePair.of(uri, lit));
        }

        jenaSolutions = new ArrayList<>(ROWS);
        String queryStr = "PREFIX ex: <" + EX + ">\n" +
                          "SELECT * WHERE { ?x ex:value ?y. }";
        jenaExecution = QueryExecutionFactory.create(queryStr, model);
        ResultSet resultSet = jenaExecution.execSelect();
        jenaSolutionFactory = new JenaSolution.Factory(resultSet.getResultVars());
        while (resultSet.hasNext())
            jenaSolutions.add(resultSet.next());
        jenaSolutions.sort(Comparator.comparing(s -> s.get("y").asLiteral().getInt()));

        stdSolutions = new  ArrayList<>(ROWS);
        for (int i = 0; i < ROWS; i++) {
            stdSolutions.add(ImmutablePair.of(new StdURI(EX+i),
                                              StdLit.fromUnescaped(String.valueOf(i), xsdInt)));
        }
        arraySolutionFactory = ArraySolution.forVars(asList("x", "y"));
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        jenaExecution.close();
    }

    @Benchmark
    public Set<Solution> hashSetOfMapSolutionsWithStdTerms() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        for (ImmutablePair<Term, Term> pair : stdSolutions) {
            set.add(MapSolution.builder().put("x", pair.left)
                                         .put("y", pair.right).build());
        }
        return set;
    }

    @Benchmark
    public Set<Solution> hashSetOfArraySolutionsWithStdTerms() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        for (ImmutablePair<Term, Term> pair : stdSolutions)
            set.add(arraySolutionFactory.fromValues(asList(pair.left, pair.right)));
        return set;
    }

    @Benchmark
    public Set<Solution> hashSetOfMapSolutionsWithJenaTerms() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        for (ImmutablePair<RDFNode, RDFNode> pair : jenaSolutionsTerms) {
            set.add(MapSolution.builder().put("x", fromJena(pair.left))
                                         .put("y", fromJena(pair.right)).build());
        }
        return set;
    }

    @Benchmark
    public Set<Solution> hashSetOfArraySolutionsWithJenaTerms() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        for (ImmutablePair<RDFNode, RDFNode> pair : jenaSolutionsTerms) {
            List<Term> list = asList(fromJena(pair.left), fromJena(pair.right));
            set.add(arraySolutionFactory.fromValues(list));
        }
        return set;
    }

    @Benchmark
    public Set<Solution> hashSetOfJenaQuerySolutions() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        for (QuerySolution jenaSolution : jenaSolutions)
            set.add(jenaSolutionFactory.transform(jenaSolution));
        return set;
    }

    @Benchmark
    public Set<Solution> hashSetOfArraySolutionsFromJenaQuerySolutions() {
        HashSet<Solution> set = new HashSet<>((int)Math.ceil(ROWS/0.75)+1);
        for (QuerySolution jenaSolution : jenaSolutions)
            set.add(arraySolutionFactory.fromFunction(n -> fromJena(jenaSolution.get(n))));
        return set;
    }
}
