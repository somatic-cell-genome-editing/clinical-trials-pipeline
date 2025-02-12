package publications;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Attribute;
import org.jsoup.nodes.Attributes;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.parser.Parser;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class PMCCrawler {

    public PMCCrawler(){}

    public static List<String> articleIdList(String text){

        Document xmlDoc= Jsoup.parse(text, "", Parser.xmlParser());
        List<String> idList=new ArrayList<String>();

        for(Element e:xmlDoc.getElementsByTag("ns1:articleidList"))
            for(Element e1:e.getElementsByTag("ns1:articleid"))
                idList.add(e1.html().toString());

        return idList;
    }
    public static String mapPMID(String pmid){
        String pmcId = "";

        try
        {
            // --- download XML page ---
            String base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=pubmed&id=";
            String url  = base + pmid;
            URL u = new URL(url);
            InputStream is  = u.openStream();
            BufferedReader dis = new BufferedReader(new InputStreamReader(is));
            String s;


            while ( (s = dis.readLine()) != null )
            {
           //  System.out.println(s);
                Document doc = Jsoup.parse(s);
                Elements elements=doc.getElementsByTag("Item");
                for(Element element:elements){
                   String text=element.text();
                   Attributes name= element.attributes();
                    for (Attribute attribute : name) {
                        if (attribute.getKey().equalsIgnoreCase("Name") && attribute.getValue().equalsIgnoreCase("pmc")) {
                           // System.out.println("PMCID:" + element.wholeText());
                            return element.wholeText();
                        }
                    }

                }

            }
            dis.close();
        }

        catch (Exception e) {System.out.println(e.toString());}

        return pmcId;
    }
    public  static void main(String[] args){
        String result=PMCCrawler.mapPMID("32541958");
        System.out.println("RESULT:"+result);
    }
}
