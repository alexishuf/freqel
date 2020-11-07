package br.ufsc.lapesd.riefederator.cnc;

import com.google.common.base.Splitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleLoader {

    private long lastId = -1;

    private long getNextId() {
        return ++lastId;
    }

    public Map<Long, Sample>
    readSampleMapFromCSV(BufferedReader br, String separator) throws IOException {
        Map<Long,Sample> samples = new HashMap<>();

        // read the first line from the CSV file
        String header = br.readLine();
        List<String> paramNames = Splitter.onPattern(separator).splitToList(header);

        // read the second line (the first line with data)
        String line = br.readLine();
        // loop until all lines are read
        while (line != null) {
            Sample sample = new Sample();

            // Set the id of this sample
            sample.setId(this.getNextId());

            // use string.split to get the values of parameters from each line of the file
            List<String> paramValues = Splitter.onPattern(separator).splitToList(line);

            // Add all values, except the last one, to a map of parameters
            Map<String,Double> map = new HashMap<>();

            for(String paramValue : paramValues.subList(0,paramValues.size()-1)) {
                map.put(paramNames.get(map.size()),Double.parseDouble(paramValue));
            }
            sample.setParameter(map);

            // Add last value read from file as Machining Process
            sample.setMachiningProcess(paramValues.get(paramValues.size()-1));

            samples.put (sample.getId(), sample );
            line = br.readLine();
        }
        return samples;
    }
}
