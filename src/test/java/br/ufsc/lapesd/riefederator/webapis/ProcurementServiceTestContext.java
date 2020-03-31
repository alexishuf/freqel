package br.ufsc.lapesd.riefederator.webapis;

import br.ufsc.lapesd.riefederator.TestContext;
import br.ufsc.lapesd.riefederator.model.term.std.StdPlain;

import javax.annotation.Nonnull;

public interface ProcurementServiceTestContext extends TestContext {
    @Nonnull StdPlain id = new StdPlain("id");
    @Nonnull StdPlain dataAbertura = new StdPlain("dataAbertura");
    @Nonnull StdPlain situacaoCompra = new StdPlain("situacaoCompra");
    @Nonnull StdPlain codigo = new StdPlain("codigo");
    @Nonnull StdPlain descricao = new StdPlain("descricao");
    @Nonnull StdPlain valor = new StdPlain("valor");
    @Nonnull StdPlain unidadeGestora = new StdPlain("unidadeGestora");
    @Nonnull StdPlain orgaoVinculado = new StdPlain("orgaoVinculado");
    @Nonnull StdPlain codigoSIAFI = new StdPlain("codigoSIAFI");
}
