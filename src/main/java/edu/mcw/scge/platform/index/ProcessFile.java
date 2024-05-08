package edu.mcw.scge.platform.index;


import com.google.gson.Gson;
import edu.mcw.scge.services.es.ESClient;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.Cell;

import org.apache.poi.ss.usermodel.Comment;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.xcontent.XContentType;
import org.json.JSONArray;
import org.json.JSONObject;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class ProcessFile {
    IndexAdmin indexer=new IndexAdmin();
    public static void main(String[] args) throws Exception {

        ProcessFile process=new ProcessFile();
        try {

            process.indexFromFile("data/GT_tracker_050124.xlsx");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Done!!");
    }
    public String parseFile(String file, String sheetName) throws Exception {
        FileInputStream fs=new FileInputStream(new File(file));
        XSSFWorkbook workbook=new XSSFWorkbook(fs);
        XSSFSheet sheet=workbook.getSheet(sheetName);
        SimpleDateFormat dateFormat=new SimpleDateFormat("MM/dd/yyy");
        Row headerRow=sheet.getRow(2);
        StringBuilder sb=new StringBuilder();

        sb.append("{\"studies\":[");
        boolean firstRow=true;
        String sponsor=null;
     loop:   for (Row row : sheet) {

            Iterator<Cell> cellIterator = row.cellIterator();
            String NCTNumber= String.valueOf(row.getCell(5));

            if (row.getRowNum() >2 ) {

                if(NCTNumber.isEmpty()){
                    sponsor=row.getCell(1).toString();
                    continue loop;
                }

                if(firstRow) {
                    sb.append("{");
                    firstRow=false;
                }else{
                    sb.append(",{");
                }
                sb.append("\"sponsor\":\"").append(sponsor).append("\",");
                boolean first=true;
                StringBuilder notes=new StringBuilder();
                while (cellIterator.hasNext() && !NCTNumber.equals("")) {
                    Cell cell = cellIterator.next();
                    int colIndex = cell.getColumnIndex();
                    if(cell.getCellComment()!=null && !cell.getCellComment().toString().isEmpty()){
                            Comment comment=cell.getCellComment();
                           String str=comment.getString().toString();
                           String substr= Arrays.stream(str.substring(str.indexOf("Comment:")+9).split("Reply:")).map(String::trim).collect(Collectors.joining(";"));
                            notes.append(substr.replaceAll("\\s+", " ")).append(" ");



                    }
                    if (headerRow.getCell(colIndex) != null && !headerRow.getCell(colIndex).toString().isEmpty() ) {
                    String columnHeader = String.valueOf(headerRow.getCell(colIndex)).replaceAll(" ", "").replaceAll(":", "");
                        if (first) {
                            sb.append("\"").append(StringUtils.uncapitalize(columnHeader)).append("\":");
                            first = false;
                        } else {
                            sb.append(",").append("\"").append(StringUtils.uncapitalize(columnHeader)).append("\":");

                        }

                        if (cell.getCellType() == Cell.CELL_TYPE_NUMERIC) {

                            sb.append("\"").append(dateFormat.format(new Date(String.valueOf(cell.getDateCellValue())))).append("\"");

                        } else if (cell.getCellType() == Cell.CELL_TYPE_FORMULA) {
                            if (colIndex == 13) {
                                sb.append("\"").append((int) (cell.getNumericCellValue() * 100)).append("%").append("\"");

                            } else {
                                sb.append("\"").append((int) (cell.getNumericCellValue())).append("\"");

                            }
                        } else {
                            try {
                                sb.append("\"").append(cell.getStringCellValue()).append("\"");
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }


                    }
                }
                if(!notes.isEmpty())
                sb.append(",\"notes\":").append("\"").append(notes).append("\"");
                sb.append("}");
            }

        }
        sb.append("]}");
        System.out.println(sb.toString());

        fs.close();
        return sb.toString();

    }
    public void index(String sb) throws IOException {
        ;
        JSONObject jsonObject = new JSONObject(sb);
        JSONArray array = (JSONArray) jsonObject.get("studies");
        System.out.println("ARRAY SIZE:"+ array.length());
        for (Object object : array) {
//            indexer.indexDocuments(object);
            IndexRequest request=   new IndexRequest(Index.getNewAlias()).source(object.toString(), XContentType.JSON);
            ESClient.getClient().index(request, RequestOptions.DEFAULT);
        }

        RefreshRequest refreshRequest = new RefreshRequest();
        ESClient.getClient().indices().refresh(refreshRequest, RequestOptions.DEFAULT);
    }
    public void indexFromFile(String file) throws Exception {
       String json= parseFile(file, "tracker_for_profit");
        index(json);
        System.out.println("DONE!!");
    }
}
