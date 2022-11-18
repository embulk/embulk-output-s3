package org.embulk.output.s3;


import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.Task;

import java.util.Optional;

/**
 * HttpProxy is config unit for Input/Output plugins' configs.
 *
 * TODO: This unit will be moved to embulk/embulk-plugin-units.git.
 * TODO: Consider using @JsonProperty(defaultValue=...) in Jackson 2.6+.
 */
public interface HttpProxy
        extends Task
{
    @Config("host")
    String getHost();

    @Config("port")
    @ConfigDefault("null")
    Optional<Integer> getPort();

    @Config("https")
    @ConfigDefault("true")
    boolean getHttps();

    @Config("user")
    @ConfigDefault("null")
    Optional<String> getUser();

    @Config("password")
    @ConfigDefault("null")
    Optional<String> getPassword();
}
