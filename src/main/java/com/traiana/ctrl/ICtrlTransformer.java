package com.traiana.ctrl;

/**
 * Created by andrewm on 11/12/2015.
 */
import java.util.Optional;

public interface ICtrlTransformer {
    void init(String configName,long flowID);
    Optional<String> transform(String msg);
}
