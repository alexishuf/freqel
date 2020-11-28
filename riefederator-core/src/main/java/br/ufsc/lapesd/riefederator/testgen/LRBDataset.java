package br.ufsc.lapesd.riefederator.testgen;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

public enum LRBDataset {
    LINKED_TCGAM,
    LINKED_TCGAE,
    LINKED_TCGAA,
    CHEBI,
    DBPEDIA,
    DRUGBANK,
    GEONAMES,
    JAMENDO,
    KEGG,
    LINKEDMDB,
    NYT,
    SWDF,
    AFFYMETRIX;

    public @Nonnull String baseName() {
        switch (this) {
            case LINKED_TCGAM:
                return "LinkedTCGA-M";
            case LINKED_TCGAE:
                return "LinkedTCGA-E";
            case LINKED_TCGAA:
                return "LinkedTCGA-A";
            case CHEBI:
                return "ChEBI";
            case DBPEDIA:
                return "DBPedia-Subset";
            case DRUGBANK:
                return "DrugBank";
            case GEONAMES:
                return "GeoNames";
            case JAMENDO:
                return "Jamendo";
            case KEGG:
                return "KEGG";
            case LINKEDMDB:
                return "LMDB";
            case NYT:
                return "NYT";
            case SWDF:
                return "SWDFood";
            case AFFYMETRIX:
                return "Affymetrix";
            default:
                break;
        }
        throw new IllegalArgumentException();
    }

    public @Nullable File getDumpFile(@Nonnull File dir) {
        for (String ext : asList("7z", "zip", "rar")) {
            File file = new File(dir, baseName() + "." + ext);
            if (file.exists()) return file;
            file = new File(dir, baseName() + ext.toUpperCase());
            if (file.exists()) return file;
        }
        return null;
    }

    public @Nonnull File getNTFile(@Nonnull File outDir) {
        return new File(outDir, "data/" + baseName() + ".nt");
    }

    public @Nonnull Writer openNTFileWriter(@Nonnull File outDir) throws FileNotFoundException {
        File file = getNTFile(outDir);
        return new OutputStreamWriter(new FileOutputStream(file, true), UTF_8);
    }
}
