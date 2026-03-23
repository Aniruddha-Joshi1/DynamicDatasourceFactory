package com.datamigrate.exportimportservice.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ConnectionField {
    private String name;
    private String label;
    private String type;
    private boolean required;
    private Object defaultValue;
    private String placeholder;
    private String description;
    private List<String> options;
}
