package io.saagie.job.template.filedownload;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.spi.filesystem.CSVFileReader;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Parameters for tests:
 * --hdfs-url hdfs://192.168.54.10:8020
 * --schema-file /data/schemas/cross-operations.avsc
 * -d /tmp/extract_operations.csv
 * -f csv
 * -s http://localhost:4545/operations.csv
 * --hive-url jdbc:hive2://localhost:10000/;ssl=false
 * --hive-table default.cross_ope
 */
public class DataImporter {

    private static final Logger LOGGER = Logger.getLogger("data-importer");

    private static String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    enum Format { csv, json }

    @Option(name = "-s", required = true, usage = "Set source url")
    private String sourceUrl;

    @Option(name = "-f", required = true, usage = "Set source format")
    private Format sourceFormat;

    @Option(name = "-d", required = true, usage = "Set destination file name (in HDFS only)")
    private String destFileName;

    @Option(name = "--hdfs-url", usage = "Set HDFS URL")
    private String hdfsUrl = "hdfs://nn1:8020";

    @Option(name = "--hdfs-user", usage = "Set HDFS user")
    private String hdfsUser = "hdfs";

    @Option(name = "--hive-table", usage = "Set Hive table to create (database.table)")
    private String hiveTable;

    @Option(name = "--hive-url", usage = "Set Hive JDBC url")
    private String hiveUrl = "jdbc:hive2://nn1:10000/;ssl=false";

    @Option(name = "--schema-file", usage = "Set schema file name (in HDFS only)")
    private String schemaFileName;

    @Option(name = "--source-charset", usage = "Set source charset")
    private String sourceCharset = "UTF-8";

    @Option(name = "--source-delim", usage = "Set source delimiter (csv)")
    private String sourceDelimiter = ",";

    @Option(name = "--source-quote", usage = "Set source quote (csv)")
    private String sourceQuote = "\"";

    @Option(name = "--source-esc", usage = "Set source escape char (csv)")
    private String sourceEscape = "\\";

    @Option(name = "--source-with-header", usage = "Set source with header (csv)")
    private boolean sourceHasHeader = true;

    private HdfsClient hdfs;

    private SchemaLoader schemaLoader;

    public static void main(String[] args) {

        CmdLineParser parser = null;

        try {
            DataImporter importer = new DataImporter();
            parser = new CmdLineParser(importer);

            parser.parseArgument(args);
            importer.init();
            importer.run();
        }
        catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
        catch (ImportException | IOException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    public void init() throws IOException {
        initHdfs();
        initSchemaLoader();
    }

    private void initHdfs() throws IOException {
        hdfs = new HdfsClient(hdfsUrl, hdfsUser);
    }

    private void initSchemaLoader() {
        schemaLoader = new SchemaLoader(hdfs, SchemaLoader.Fallback.LOCAL_ONLY);
    }

    private void run() throws ImportException, IOException {

        Schema schema = null;

        if (StringUtils.isNoneEmpty(schemaFileName)) {
            schema = schemaLoader.get(schemaFileName);

            if (schema == null) {
                LOGGER.warning("Unable to validate the data");
            }
        }

        String tmpFileName = "/tmp/" + System.currentTimeMillis();

        // 1- Get source
        //
        downloadSource(tmpFileName);

        // 2- Check if each row is valid if a schema is provided
        if (schema != null) {
            switch (sourceFormat) {
                case csv:
                    validateCsv(tmpFileName, schema);
                    break;
                case json:
                    validateJson(tmpFileName, schema);
                    break;
            }
        }

        // 3- Copy file in HDFS
        copyInHdfs(tmpFileName);

        // 4- If needed, create a hive table (use the schema if provided,
        //    otherwise use the first row for the field names and set the type to string)

        if (this.sourceFormat == Format.csv) {
            createHiveTable(schema);
        }
    }

    private void copyInHdfs(String tmpFileName) throws IOException {
        hdfs.write(tmpFileName, this.destFileName);
    }

    /**
     * This method only works for CSV file at the moment.
     *
     * @param schema
     */
    private void createHiveTable(Schema schema) {

        try {
            Class.forName(HIVE_DRIVER);

            Connection conn = DriverManager.getConnection(this.hiveUrl, "", "");
            Statement stmt = conn.createStatement();

            stmt.execute("DROP TABLE IF EXISTS " + this.hiveTable);

            StringBuilder buffer = new StringBuilder();
            buffer.append("CREATE EXTERNAL TABLE ").append(this.hiveTable).append(" (\n");

            if (schema != null) {
                for (Schema.Field field : schema.getFields()) {
                    buffer.append("  `").append(field.name()).append("` ");
                    buffer.append(getSqlType(field)).append(",\n");
                }
                buffer.replace(buffer.length() - 2, buffer.length(), ")\n");
            }
            else if (this.sourceHasHeader){
                // No schema: read first row (header) and each field will be a string
            }
            else {
                LOGGER.severe("Cannot create hive table without information about the fields");
            }

            buffer.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','\n");
            buffer.append("LOCATION '").append(FilenameUtils.getFullPath(this.destFileName)).append("'\n");

            if (this.sourceHasHeader) {
                buffer.append("TBLPROPERTIES ('skip.header.line.count'='1')\n");
            }

            LOGGER.fine(buffer.toString());

            stmt.execute(buffer.toString());
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private String getSqlType(Schema.Field field) {

        String sqlType = null;
        Schema.Type type = field.schema().getType();

        switch (type) {
            case UNION:
                List<Schema> schemas = field.schema().getTypes();
                for (Schema schema: schemas) {
                    sqlType = schema.getType().name().equalsIgnoreCase("null") ? sqlType : schema.getType().name();
                }
                break;
            case MAP:
            case FIXED:
            case ENUM:
            case RECORD:
            case NULL:
            case ARRAY:
                LOGGER.warning("Type " + type + " ignored");
                break;
            case LONG:
                sqlType = "BIGINT";
                break;
            default:
                sqlType = type.name();
                break;
        }

        return sqlType;
    }

    private void downloadSource(String tmpFileName) throws ImportException, IOException {

        if (!sourceUrl.startsWith("http")) {
            throw new ImportException("Only HTTP(S) is supported at the moment");
        }
        else {
            LOGGER.info("Downloading " + sourceUrl + " ...");
            HttpClient httpClient = new HttpClient();
            GetMethod method = new GetMethod(sourceUrl);
            int status = httpClient.executeMethod(method);

            if (status == 200) {
                LOGGER.info("Save source in " + tmpFileName);
                InputStream body = method.getResponseBodyAsStream();
                File target = new File(tmpFileName);
                Files.copy(body, target.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
            else {
                throw new ImportException("Status is " + status);
            }
        }
    }

    private boolean validateJson(String jsonFileName, Schema schema) throws IOException {

        String json = FileUtils.readFileToString(new File(jsonFileName), this.sourceCharset);
        InputStream input = new ByteArrayInputStream(json.getBytes());
        DataInputStream din = new DataInputStream(input);

        try {
            DatumReader reader = new GenericDatumReader(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            reader.read(null, decoder);
            return true;
        }
        catch (AvroTypeException | IOException exc) {
            LOGGER.warning("Cannot validate JSON: " + exc.getMessage());
            return false;
        }
    }

    private void validateCsv(String csvFileName, Schema schema) throws IOException, ImportException {

        InputStream is = new FileInputStream(csvFileName);

        CSVProperties props = new CSVProperties.Builder()
                .charset(this.sourceCharset)
                .delimiter(this.sourceDelimiter)
                .quote(this.sourceQuote)
                .escape(this.sourceEscape)
                .hasHeader(this.sourceHasHeader)
                .linesToSkip(0)
                .build();

        AtomicLong written = new AtomicLong(0L);
        List<DatasetRecordException> failures = new ArrayList<>();

        DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(newDatumWriter(schema, GenericData.Record.class));

        CSVFileReader<GenericData.Record> reader = new CSVFileReader<>(is, props, schema, GenericData.Record.class);

        reader.initialize();

        try {
            LOGGER.info("Start reading file to validate it ...");
            while (reader.hasNext()) {
                try {
                    reader.next();
                    written.incrementAndGet();
                }
                catch (DatasetRecordException e) {
                    LOGGER.warning("Validation error: " + e.getMessage());
                    failures.add(e);
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        LOGGER.info("End of validation: failures=" + failures.size());

        if (failures.size() != 0) {
            String errOutput = "/tmp/" + System.currentTimeMillis() + ".err";
            FileOutputStream errOut = new FileOutputStream(errOutput);
            errOut.write(failures.toString().getBytes());
            errOut.close();
            throw new ImportException("Some rows are not valid. The result is stored in " + errOutput);
        }
    }

    public static <D> DatumWriter<D> newDatumWriter(Schema schema, Class<D> dClass) {
        return (DatumWriter<D>) GenericData.get().createDatumWriter(schema);
    }
}

