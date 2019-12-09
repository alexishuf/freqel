package br.ufsc.lapesd.riefederator.description.molecules;

import br.ufsc.lapesd.riefederator.description.Molecule;
import br.ufsc.lapesd.riefederator.model.term.Term;
import org.jetbrains.annotations.Contract;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.Set;

public class MoleculeBuilder {
    private final @Nonnull String name;
    private boolean exclusive = false;
    private @Nonnull Set<MoleculeLink> in = new HashSet<>(), out = new HashSet<>();

    public MoleculeBuilder(@Nonnull String name) {
        this.name = name;
    }

    @Contract("-> this") public @Nonnull MoleculeBuilder nonExclusive() {return exclusive(false);}
    @Contract("-> this") public @Nonnull MoleculeBuilder    exclusive() {return exclusive(true);}
    @Contract("_ -> this")
    public @Nonnull MoleculeBuilder exclusive(boolean value) {
        this.exclusive = value;
        return this;
    }


    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom) {
        return in(edge, atom, false);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder inAuthoritative(@Nonnull Term edge, @Nonnull Atom atom) {
        return in(edge, atom, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder in(@Nonnull Term edge, @Nonnull Atom atom, 
                                       boolean authoritative) {
        this.in.add(new MoleculeLink(edge, atom, authoritative));
        return this;
    }


    @Contract("_, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom) {
        return out(edge, atom, false);
    }
    @Contract("_, _-> this")
    public @Nonnull MoleculeBuilder outAuthoritative(@Nonnull Term edge, @Nonnull Atom atom) {
        return out(edge, atom, true);
    }
    @Contract("_, _, _ -> this")
    public @Nonnull MoleculeBuilder out(@Nonnull Term edge, @Nonnull Atom atom,
                                       boolean authoritative) {
        this.out.add(new MoleculeLink(edge, atom, authoritative));
        return this;
    }


    @Contract("-> new") public @Nonnull Atom buildAtom() {
        return new Atom(name, exclusive, in, out);
    }
    @Contract("-> new") public @Nonnull Molecule build() { return new Molecule(buildAtom()); }
}
