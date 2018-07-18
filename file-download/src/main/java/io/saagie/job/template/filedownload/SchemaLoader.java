package io.saagie.job.template.filedownload;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;

public class SchemaLoader {

    enum Fallback { LOCAL_IF_EXCEPTION, LOCAL_ONLY }

    private HdfsClient hdfs;

    private Fallback fallback;

    private Schema.Parser schemaParser = new Schema.Parser();

    public SchemaLoader(HdfsClient hdfs, Fallback fallback) {
        this.hdfs = hdfs;
        this.fallback = fallback;
    }

    public Schema get(String schemaFileName) {
        Schema schema = null;
        String schemaJson = null;

        switch (fallback) {
            case LOCAL_IF_EXCEPTION:
                try {
                    schemaJson = hdfs.readAsString(schemaFileName);
                }
                catch (IOException e) {
                    schemaJson = getLocalSchemaFile(schemaFileName);
                }
                break;
            case LOCAL_ONLY:
                schemaJson = getLocalSchemaFile(schemaFileName);
        }

        if (StringUtils.isNoneEmpty(schemaJson)) {
            schema = schemaParser.parse(schemaJson);
        }

        return schema;
    }

    private String getLocalSchemaFile(String schemaFileName) {
        try {
            return FileUtils.readFileToString(new File(schemaFileName), "UTF-8");
        }
        catch (IOException e) {
            return null;
        }
    }
}
