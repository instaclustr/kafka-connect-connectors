package com.instaclustr.kafka.connect.s3;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.regex.Pattern;

/**
 * This class can be used to validate user config values using regex
 */

public class RegexStringValidator implements ConfigDef.Validator {
    private final Pattern pattern;
    private final String validationFailMessage;


    public RegexStringValidator(final Pattern pattern, final String validationFailMessage) {
        this.pattern = pattern;
        this.validationFailMessage = validationFailMessage;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (!pattern.matcher((String) value).find()) {
            throw new ConfigException(String.format("%s: %s", name, validationFailMessage));
        }
    }
}
