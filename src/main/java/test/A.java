package test;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.io.IOException;

public class A implements DeserializationSchema<Integer> {


    public static void main(String[] args) {


        ConfigOptions.OptionBuilder hostname = ConfigOptions.key("hostname");
        ConfigOptions.TypedConfigOptionBuilder<String> stringType = hostname.stringType();
        ConfigOption<String> stringConfigOption = stringType.noDefaultValue();

    }

    @Override
    public Integer deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(Integer integer) {
        return false;
    }

    @Override
    public TypeInformation<Integer> getProducedType() {
        return null;
    }
}
