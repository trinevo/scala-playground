package pl.mtpl.spark;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.json.JSONObject;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by MarcinT.P on 2017-02-25.
 * This uses hard-coded schema
 */
public class AvroTuppleParser {

    private JSONObject createSchema() {
        return new JSONObject("{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"no\", \"type\": \"string\"},\n" +
                "     {\"name\": \"name\",  \"type\": \"string\"},\n" +
                "     {\"name\": \"surname\",  \"type\": \"string\"},\n" +
                "     {\"name\": \"is_male\",  \"type\": \"string\"},\n" +
                "     {\"name\": \"age\",  \"type\": \"string\"},\n" +
                " ]\n" +
                "}");
    }

    private void loadTuple(List<GenericRecord> users, String fileName) {
        int records = 0;
        try {
            System.out.println("Load tupple from " + fileName);
            File file = new File(fileName);
            Schema schema = new Schema.Parser().parse(createSchema().toString());
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
            while (dataFileReader.hasNext()) {
                GenericRecord user = dataFileReader.next();
                users.add(user);
                ++records;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("From " +  fileName + " loaded " + records + " tupples, " + users.size() + " so far");
        }
    }

    public void loadTuples(String directoryName) {
        List<GenericRecord> users = new LinkedList<GenericRecord>();
        int totalFiles = 0;
        try {
            System.out.println("Load tupple from directory" + directoryName);
            File[] files = new File(directoryName).listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getAbsolutePath().endsWith(".avro");
                }
            });
            if(files != null) {
                for(File f : files) {
                    loadTuple(users, f.getAbsolutePath());
                    ++totalFiles;
                }
            }
        } finally {
            System.out.println("From " + directoryName + " via " + totalFiles + " files, total " + users.size());
        }
    }
}
