package br.ufsc.lapesd.riefederator.cnc;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SampleMatcher {
    public static List<Map<String,Object>> filterSamples(List<Sample> sampleList,
                                                         MultivaluedMap<String, String> queryParams,
                                                         UriInfo uriInfo) {
        List<Map<String,Object>> workingSet = new ArrayList<>();
        if (queryParams == null || queryParams.size() == 0) {
            sampleList.forEach(sample -> workingSet.add(sample.toMap(uriInfo, sample.getId())));
        } else {
            for (Sample sample : sampleList) {
                boolean added = false;
                for (String param : queryParams.keySet()) {
                    switch (param) {
                        case "id":
                            for (String value : queryParams.get(param)) {
                                if (value.contains("|")) {  // A value range was specified
                                    String[] range = value.split(Pattern.quote("|"));
                                    if (range.length < 1 || range.length > 2)
                                        throw new NumberFormatException();
                                    long min = range[0].equals("") ? Long.MIN_VALUE : Long.parseLong(range[0]);
                                    long max = range.length == 1 ? Long.MAX_VALUE : Long.parseLong(range[1]);
                                    if (sample.getId() >= min && sample.getId() <= max) {
                                        workingSet.add(sample.toMap(uriInfo, sample.getId()));
                                        added = true;
                                    }
                                } else { // An exact value was specified
                                    if (Long.parseLong(value) == sample.getId()) {
                                        workingSet.add(sample.toMap(uriInfo, sample.getId()));
                                        added = true;
                                    }
                                }
                                if (added) break;
                            }
                            break;
                        case "machiningProcess":
                            if (queryParams.get(param).contains(sample.getMachiningProcess())) {
                                workingSet.add(sample.toMap(uriInfo, sample.getId()));
                                added = true;
                            }
                            break;
                        case "productModel":
                            if (queryParams.get(param).contains(sample.getProductModel())) {
                                workingSet.add(sample.toMap(uriInfo, sample.getId()));
                                added = true;
                            }
                            break;
                        case "serialNumber":
                            if (queryParams.get(param).contains(sample.getSerialNumber())) {
                                workingSet.add(sample.toMap(uriInfo, sample.getId()));
                                added = true;
                            }
                            break;
                        default:  // All parameters
                            for (String value : queryParams.get(param)) {
                                if (value.contains("|")) {  // A value range was specified
                                    String[] range = value.split(Pattern.quote("|"));
                                    if (range.length < 1 || range.length > 2)
                                        throw new NumberFormatException();
                                    double min = range[0].equals("") ? Double.MIN_VALUE : Double.parseDouble(range[0]);
                                    double max = range.length == 1 ? Double.MAX_VALUE : Double.parseDouble(range[1]);
                                    if (sample.getParameter().get(param) >= min && sample.getParameter().get(param) <= max) {
                                        workingSet.add(sample.toMap(uriInfo, sample.getId()));
                                        added = true;
                                    }
                                } else { // An exact value was specified
                                    if (value.equals(sample.getParameter().get(param).toString())) {
                                        workingSet.add(sample.toMap(uriInfo, sample.getId()));
                                        added = true;
                                    }
                                }
                                if (added) break;
                            }
                    }
                    if (added) break;
                }
            }
        }
        return workingSet;
    }
}
