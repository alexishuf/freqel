package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;

import javax.annotation.Nonnull;

public interface TransparencyServiceTestContext extends TestContext {
    @Nonnull StdPlain id = new StdPlain("id");
    @Nonnull StdPlain dataAbertura = new StdPlain("dataAbertura");
    @Nonnull StdPlain dataInicioVigencia = new StdPlain("dataInicioVigencia");
    @Nonnull StdPlain dataFimVigencia = new StdPlain("dataFimVigencia");
    @Nonnull StdPlain situacaoCompra = new StdPlain("situacaoCompra");
    @Nonnull StdPlain codigo = new StdPlain("codigo");
    @Nonnull StdPlain numero = new StdPlain("numero");
    @Nonnull StdPlain descricao = new StdPlain("descricao");
    @Nonnull StdPlain valor = new StdPlain("valor");
    @Nonnull StdPlain unidadeGestora = new StdPlain("unidadeGestora");
    @Nonnull StdPlain licitacao = new StdPlain("licitacao");
    @Nonnull StdPlain modalidadeLicitacao = new StdPlain("modalidadeLicitacao");
    @Nonnull StdPlain orgaoVinculado = new StdPlain("orgaoVinculado");
    @Nonnull StdPlain valorInicialCompra = new StdPlain("valorInicialCompra");
    @Nonnull StdPlain codigoSIAFI = new StdPlain("codigoSIAFI");
}
