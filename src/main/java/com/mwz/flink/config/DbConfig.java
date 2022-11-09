package com.mwz.flink.config;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author mwz
 */
@Data
@Accessors(chain = true)
public class DbConfig {

    public static String hostname = "localhost";

    public static String username = "root";

    public static String password = "123456";

}
