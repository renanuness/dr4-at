package br.com.freela.contrato.domain.helper;


import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

public class CustomJsonMapper {

    public static JsonMapper instance(){
        JsonMapper onlyFeature3 = JsonMapper.builder().disable(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS).build();
        return onlyFeature3;
    }
}
