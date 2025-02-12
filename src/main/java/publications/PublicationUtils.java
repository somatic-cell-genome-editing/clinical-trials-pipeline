package publications;

import gate.Annotation;
import gate.AnnotationSet;
import gate.Document;

import java.util.ArrayList;
import java.util.List;

import static publications.Stemmer.stem;

public class PublicationUtils {
    public enum OutputType {
        TO_FILE, TO_HBASE
    }
    public List<String> annotateToOutput(OutputType output, AnnieAnnotator annotator, long pmid,
                                         String text_to_annotate, int text_location,
                                         List<String> annotation_sets, boolean useStemming) {
        //System.out.println("annotate to output: " + annotation_sets.size());
//		for (String i: annotation_sets){
//			System.out.println(i);
//		}
        Document doc;
        if (useStemming) {
            doc = annotator.process(stem(text_to_annotate));
        } else {
            doc = annotator.process1(text_to_annotate, pmid);
        }
        List<String> outputStr = new ArrayList<String>();
        if (doc != null) {
            try {
                for (String anns_set_name : annotation_sets) {
                    AnnotationSet anns = doc.getAnnotations(anns_set_name);
                    for (Annotation ann : anns) {
                        System.out.println("first node: " + anns.firstNode());
                        System.out.println("last node: " + anns.lastNode());
                        switch (output) {
                            case TO_FILE:
                                outputStr.add(pmid + "\t" + text_location + "\t" + ann.getType() + "\t"
                                        + anns_set_name + "\t" + ann.getStartNode().getOffset() + "\t"
                                        + ann.getEndNode().getOffset() + "\t" + ann.getFeatures().toString());
                            case TO_HBASE:
                                outputStr.add(text_location + "\t" + ann.getType() + "\t"
                                        + anns_set_name + "\t" + ann.getStartNode().getOffset() + "\t"
                                        + ann.getEndNode().getOffset() + "\t" + ann.getFeatures().toString());
                            default:
                        }
                    }
                }
            } catch (Exception e) {
//                logger.error("Error in annotating: [" + text_to_annotate + "]");
//                logger.error(e);
            }
        }
        annotator.clear();
        return outputStr;
    }

}
