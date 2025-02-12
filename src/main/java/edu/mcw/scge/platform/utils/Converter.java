package edu.mcw.scge.platform.utils;

import com.sun.codemodel.JCodeModel;
import org.jsonschema2pojo.*;
import org.jsonschema2pojo.rules.RuleFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;


public class Converter {
    public void convertJsonToJavaClass(URL inputJsonUrl, File outputJavaClassDirectory, String packageName, String javaClassName)
            throws IOException {
        JCodeModel jcodeModel = new JCodeModel();

        GenerationConfig config = new DefaultGenerationConfig() {
            @Override
            public boolean isGenerateBuilders() {
                return true;
            }

            @Override
            public SourceType getSourceType() {
                return SourceType.JSON;
            }
        };

        SchemaMapper mapper = new SchemaMapper(new RuleFactory(config, new Jackson2Annotator(config), new SchemaStore()), new SchemaGenerator());
        mapper.generate(jcodeModel, javaClassName, packageName, inputJsonUrl);

        jcodeModel.build(outputJavaClassDirectory);
    }
    public static void main(String[] args) throws IOException, URISyntaxException {
       URL inputJsonUrl=new URI("file:///C:/Users/jthota/Documents/git/clinical_trails_pipeline/data/input.json").toURL();
       String packageName="edu.mcw.scge.platform.model";
       String className="Study";
       Converter converter=new Converter();
       converter.convertJsonToJavaClass(inputJsonUrl,new File("C:\\Users\\jthota\\Documents\\git\\clinical_trails_pipeline\\src\\main\\java\\"), packageName, className);
    }
}
