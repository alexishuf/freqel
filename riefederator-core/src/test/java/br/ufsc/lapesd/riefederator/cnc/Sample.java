package br.ufsc.lapesd.riefederator.cnc;

import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Sample {
    private long id;
    private Map<String,Double> parameter;
    private String machiningProcess;
    private String productModel;
    private String serialNumber;

    public String getProductModel() {
        return productModel;
    }

    public void setProductModel(String productModel) {
        this.productModel = productModel;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(String serialNumber) {
        this.serialNumber = serialNumber;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Map<String, Double> getParameter() {
        return this.parameter;
    }

    public void setParameter(Map<String, Double> params) {
        this.parameter = params;
    }

    public String getMachiningProcess() {
        return machiningProcess;
    }

    public void setMachiningProcess(String machiningProcess) {
        this.machiningProcess = machiningProcess;
    }

    public Map<String,Object> toMap(UriInfo uriInfo, long id) {
        Map<String, Object> map = this.toMap();
        if (uriInfo != null) {
            Map<String, Map<String, String>> links = new HashMap<>();
            Map<String, String> self = new HashMap<>();
            self.put("href", uriInfo.getBaseUri() + uriInfo.getPath() + "/" + id);
            links.put("self", self);
            map.put("_links", links);
        }
        return map;
    }

    public Map<String,Object> toMap(UriInfo uriInfo) {
        Map<String, Object> map = this.toMap();
        if (uriInfo != null) {
            Map<String, Map<String, String>> links = new HashMap<>();
            Map<String, String> self = new HashMap<>();
            self.put("href", uriInfo.getBaseUri() + uriInfo.getPath());
            links.put("self", self);
            map.put("_links", links);
        }
        return map;
    }

    public Map<String,Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("id", this.getId());
        map.put("machiningProcess", this.getMachiningProcess());
        map.put("productModel", this.getProductModel());
        map.put("serialNumber", this.getSerialNumber());
        map.put("type", "Sample");

        List<Map<String, Object>> list = new ArrayList<>();

        for(Map.Entry<String,Double> param: this.getParameter().entrySet()) {
            Map<String, Object> mapParam = new HashMap<>();
            mapParam.put("type", param.getKey());
            mapParam.put("value", param.getValue());
            list.add(mapParam);
        }
        map.put("parameter", list);
        return map;
    }
}
