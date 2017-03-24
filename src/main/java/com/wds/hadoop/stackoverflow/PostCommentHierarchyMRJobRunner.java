package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wangdongsong1229@163.com on 2017/3/23.
 */
public class PostCommentHierarchyMRJobRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PostCommentHierarchyMRJob");
        job.setJarByClass(PostCommentHierarchyMRJobRunner.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CommentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PostMapper.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setReducerClass(PostCommentHierarchyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 2;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new PostCommentHierarchyMRJobRunner(), args);
    }

    private class CommentMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            outKey.set(parsed.get("PostId"));
            outValue.set("C" + value.toString());
            context.write(outKey, outValue);
        }
    }

    private class PostMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Map<String, String> parsed = MRDPUtils.transFormXMLToMap(value.toString());
            outKey.set(parsed.get("Id"));
            outValue.set("P" + value.toString());
            context.write(outKey, outValue);
        }
    }

    private class PostCommentHierarchyReducer extends Reducer<Text, Text, Text, NullWritable> {
        private List<String> comments = new ArrayList<>();
        private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        private String post = null;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            post = null;
            comments.clear();

            for (Text t : values) {
                if (t.toString().startsWith("P")) {
                    post = t.toString().substring(1, t.toString().length()).trim();
                } else {
                    comments.add(t.toString().substring(1, t.toString().length()).trim());
                }
            }

            if (post != null) {
                String postWithCommentChildren = nextElements(post, comments);

                context.write(new Text(postWithCommentChildren), NullWritable.get());
            }
        }

        private String nextElements(String post, List<String> comments) {
            try {
                DocumentBuilder bldr = dbf.newDocumentBuilder();
                Document doc = bldr.newDocument();

                Element postEl = getXmlElementFromString(post);
                Element toAddPostEl = doc.createElement("post");

                copyAttributesToElement(postEl.getAttributes(), toAddPostEl);

                for (String commentXml : comments) {
                    Element commentEl = getXmlElementFromString(commentXml);
                    Element toAddCommentEl = doc.createElement("comments");
                    copyAttributesToElement(commentEl.getAttributes(), toAddCommentEl);
                    toAddPostEl.appendChild(toAddCommentEl);
                }
                doc.appendChild(toAddPostEl);
                return transformDocumentToString(doc);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (TransformerConfigurationException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            } catch (TransformerException e) {
                e.printStackTrace();
            }
            return null;
        }

        private String transformDocumentToString(Document doc) throws TransformerException {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(doc), new StreamResult(writer));
            return writer.getBuffer().toString().replaceAll("\n|\r", "");
        }

        private void copyAttributesToElement(NamedNodeMap attributes, Element toAddPostEl) {
            for (int i = 0 ; i < attributes.getLength(); ++i) {
                Attr toCopy = (Attr) attributes.item(i);
                toAddPostEl.setAttribute(toCopy.getName(), toCopy.getValue());
            }
        }

        private Element getXmlElementFromString(String post) throws ParserConfigurationException, IOException, SAXException {
            DocumentBuilder bldr = dbf.newDocumentBuilder();
            return bldr.parse(new InputSource(new StringReader(post))).getDocumentElement();
        }
    }


}
