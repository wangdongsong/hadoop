package com.wds.hadoop.stackoverflow;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
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
 * 该示例是PostCommentHierachyMRJobRunner延续，并且PostCommentHierachyMRJobRunner的输出作为本示例的输入。
 * PostCommentHierachyMRJobRunner中已经获得与帖子相关的所有评论，本例将帖子的问题与其回复关联起来。帖子中
 * 即包括回复，也包括问题，且这两者通过PostTypeId进行区分，因此可以通过问题中的Id以及回复中的ParentId将其组合在一起
 *
 * 问题：给定PostCommentHierachyMRJobRunner输出，通过自连接操作创建问题、回复及评论层次
 *
 * PostCommentHierarchyMRJobRunner的输出结果作为本类的输入
 * Created by wangdongsong1229@163.com on 2017/3/24.
 */
public class QuestionAnswerMRJobRunner extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

    public static void main(String[] args) {

    }

    /**
     * 第一件事是判断该记录是问题还是回复，因为问题和回复对应不同的操作。对于问题，取其Id作为键并将其标记为问题。
     * 对于回复，则取其ParentID作为键并标记为回复
     */
    public static class QuestionAnswerMapper extends Mapper<Object, Text, Text, Text> {
        private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                Element post = getXmlElementFromString(value.toString());
                int postType = Integer.parseInt(post.getAttribute("PostTypeId"));
                if (postType == 1) {
                    outKey.set(post.getAttribute("Id"));
                    outValue.set("Q" + value.toString());
                } else {
                    outKey.set(post.getAttribute("ParentId"));
                    outValue.set("A" + value.toString());
                }
                context.write(outKey, outValue);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }
        }
    }

    public static class QuestionAnswerReducer extends Reducer<Text, Text, Text, NullWritable> {
        private List<String> answers = new ArrayList<>();
        private DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        private String question = null;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            question = null;
            answers.clear();

            for (Text t : values) {
                if (t.toString().startsWith("Q")) {
                    question = t.toString().substring(1, t.toString().length()).trim();
                } else {
                    answers.add(t.toString().substring(1, t.toString().length()).trim());
                }
            }

            if (question != null) {
                String postWithCommentChildren = nextElements(question, answers);
                context.write(new Text(postWithCommentChildren), NullWritable.get());
            }
        }
    }
    private static String nextElements(String post, List<String> comments) {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
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

    private static String transformDocumentToString(Document doc) throws TransformerException {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(writer));
        return writer.getBuffer().toString().replaceAll("\n|\r", "");
    }

    private static void copyAttributesToElement(NamedNodeMap attributes, Element toAddPostEl) {
        for (int i = 0 ; i < attributes.getLength(); ++i) {
            Attr toCopy = (Attr) attributes.item(i);
            toAddPostEl.setAttribute(toCopy.getName(), toCopy.getValue());
        }
    }

    private static Element getXmlElementFromString(String post) throws ParserConfigurationException, IOException, SAXException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder bldr = dbf.newDocumentBuilder();
        return bldr.parse(new InputSource(new StringReader(post))).getDocumentElement();
    }

}
