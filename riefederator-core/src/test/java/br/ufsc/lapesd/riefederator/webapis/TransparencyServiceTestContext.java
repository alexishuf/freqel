package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;

import javax.annotation.Nonnull;

public interface TransparencyServiceTestContext extends TestContext {
    @Nonnull StdPlain id = new StdPlain("id");
    @Nonnull StdPlain dataAbertura = new StdPlain("dataAbertura");
    @Nonnull StdPlain dataInicioVigencia = new StdPlain("dataInicioVigencia");
    @Nonnull StdPlain dataFimVigencia = new StdPlain("dataFimVigencia");
    @Nonnull StdPlain dimCompra = new StdPlain("dimCompra");
    @Nonnull StdPlain situacaoCompra = new StdPlain("situacaoCompra");
    @Nonnull StdPlain codigo = new StdPlain("codigo");
    @Nonnull StdPlain nome = new StdPlain("nome");
    @Nonnull StdPlain contrato = new StdPlain("contrato");
    @Nonnull StdPlain numero = new StdPlain("numero");
    @Nonnull StdPlain objeto = new StdPlain("objeto");
    @Nonnull StdPlain descricao = new StdPlain("descricao");
    @Nonnull StdPlain valor = new StdPlain("valor");
    @Nonnull StdPlain unidadeGestora = new StdPlain("unidadeGestora");
    @Nonnull StdPlain licitacao = new StdPlain("licitacao");
    @Nonnull StdPlain modalidadeLicitacao = new StdPlain("modalidadeLicitacao");
    @Nonnull StdPlain modalidadeCompra = new StdPlain("modalidadeCompra");
    @Nonnull StdPlain orgaoVinculado = new StdPlain("orgaoVinculado");
    @Nonnull StdPlain valorInicialCompra = new StdPlain("valorInicialCompra");
    @Nonnull StdPlain codigoSIAFI = new StdPlain("codigoSIAFI");

    @Nonnull StdPlain hasLicitacao = new StdPlain("hasLicitacao");
}
