package com.datapipeline.starter.moka;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;


public class FileUtil {

  public static List<CSVRecord> readCSV(String filePath, String[] headers) throws IOException {
    CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(headers);
    FileReader fileReader = new FileReader(filePath);
    CSVParser parser = new CSVParser(fileReader, csvFormat);
    List<CSVRecord> records = parser.getRecords();
    parser.close();
    fileReader.close();
    return records;
  }

  public static String readFileContent(String fileName) {
    File file = new File(fileName);
    BufferedReader reader = null;
    StringBuilder sbf = new StringBuilder();
    try {
      reader = new BufferedReader(new FileReader(file));
      String tempStr;
      while ((tempStr = reader.readLine()) != null) {
        sbf.append(tempStr);
      }
      reader.close();
      return sbf.toString();
    } catch (IOException e) {
      throw new RuntimeException("读取文件失败，检查文件路径是否正确", e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e1) {
          throw new RuntimeException("关闭文件流失败", e1);
        }
      }
    }
  }

}
