## Linkedador

```
Contract:
  hasProcurement:
    organization: x---------------------------------+
    number:       y---------------------------------|----+
    modality:     z---------------------------------|----|------+
---                                                 v    v      v
http://.../licitacoes/por-uasg-numero-modalidade/{?uasg,numero,modalidade} 
```

```
Contrato:
  unidadeGestora:                             #</definitions/UnidadeGestora>
    codigo: x --------------------------+
  dimCompra:                            |     #</definitions/DimCompra>
    numero: y ------------------------+ |
  modalidadeCompra:                   | |     #</definitions/ModalidadeCompra>
    codigo: z ----------------------+ | +-----------+ 
---                                 | +-------------|----+
                                    +---------------|----+------+
                                                    v    v      v
http://..../licitacoes/por-uasg-numero-modalidade/{?uasg,numero,modalidade}
                                                    ^    ^      ^
                                        +-----------+    |      |
                                        | +--------------+      |
---                                     | | +-------------------+
Licitacao:                              | | |
  unidadeGestora:                       | | | #</definitions/UnidadeGestora>
    codigo: x --------------------------+ | |
  licitacao:                              | | #</definitions/DimCompra>
    numero: y ----------------------------+ |
  modalidadeLicitacao:                      | #</definitions/ModalidadeCompra>
    codigo: z ------------------------------+
```

```
       # |---> nome gerado por ausência de semântica
       # |     no cenário do linkedador geraria owl:sameAs
?src :temLicitacao ?dst
---
# ?src / ?srcAtom # Não aplicável nesse caso
?src :unidadeGestora/:codigo   ?j0;
     :dimCompra/:numero        ?j1;
     :modalidadeCompra/:codigo ?j2.
?dst :unidadeGestora/:codigo      ?j0;
     :licitacao/:numero           ?j1;
     :modalidadeLicitacao/:codigo ?j2.
```

