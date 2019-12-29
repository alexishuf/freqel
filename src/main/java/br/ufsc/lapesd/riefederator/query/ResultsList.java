package br.ufsc.lapesd.riefederator.query;

import br.ufsc.lapesd.riefederator.query.error.ResultsCloseException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ResultsList extends ArrayList<Results> implements AutoCloseable {
    private static final @Nonnull Logger logger = LoggerFactory.getLogger(ResultsList.class);

    public ResultsList(int initialCapacity) {
        super(initialCapacity);
    }

    public ResultsList() { }

    public ResultsList(@NotNull Collection<? extends Results> c) {
        super(c);
    }

    @Override
    public void close() throws ResultsCloseException {
        List<ResultsCloseException> exceptionList = new ArrayList<>();
        for (Results r : this) {
            try {
                r.close();
            } catch (ResultsCloseException e) {
                logger.error("Exception on close()ing Results {} in List", r, e);
                exceptionList.add(e);
            }
        }
        if (exceptionList.size() == 1)
            throw exceptionList.get(0);
        else if (exceptionList.size() > 1)
            throw new ResultsCloseException(get(0), "Multiple exceptions", exceptionList.get(0));
    }
}
