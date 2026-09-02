package interestinglab;

import com.alibaba.fastjson.JSON;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;
import io.github.interestinglab.waterdrop.config.ConfigRenderOptions;
import io.github.interestinglab.waterdrop.config.ConfigResolveOptions;

import java.io.File;
import java.net.URL;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
public class CompleteTests {

  public static void main(String[] args) throws Exception {

    System.out.println("Hello World");

  System.setProperty("dt", "20190318");
   System.setProperty("city2", "shanghai");
    System.setProperty("trueVal", "false");
    System.setProperty("booleans.falseVal", "shanghai");


    CompleteTests completeTests = new CompleteTests();
    File fileFromResources = completeTests.getFileFromResources("interestinglab/variables.conf");

    //File fileFromResources = completeTests.getFileFromResources("beanconfig/beanconfig01.conf");

    Config config = ConfigFactory.parseFile(fileFromResources);

      String jsonDoc = "{\n" +
              "    \"spark\": {\n" +
              "        \"spark.streaming.batchDuration\": 5,\n" +
              "        \"spark.app.name\": \"Waterdrop\",\n" +
              "        \"spark.executor.instances\": 2,\n" +
              "        \"spark.executor.cores\": 1,\n" +
              "        \"spark.executor.memory\": \"1g\"\n" +
              "    },\n" +
              "    \"input\": [\n" +
              "        {\n" +
              "            \"rate\": 1,\n" +
              "            \"plugin_name\": \"fakestream\",\n" +
              "            \"content\": [\n" +
              "                \"20190318, beijing, first message\",\n" +
              "                \"20190319, shanghai, second message\",\n" +
              "                \"20190318, shanghai, third message\"\n" +
              "            ]\n" +
              "        }\n" +
              "    ],\n" +
              "    \"filter\": [\n" +
              "        {\n" +
              "            \"delimiter\": \",\",\n" +
              "            \"fields\": [\n" +
              "                \"dt\",\n" +
              "                \"city\",\n" +
              "                \"msg\"\n" +
              "            ],\n" +
              "            \"plugin_name\": \"split\"\n" +
              "        },\n" +
              "        {\n" +
              "            \"result_table_name\": \"result1\",\n" +
              "            \"plugin_name\": \"sql\",\n" +
              "            \"table_name\": \"user_view\",\n" +
              "            \"sql\": \"select * from user_view where city = 'beijing'\"\n" +
              "        },\n" +
              "        {\n" +
              "            \"result_table_name\": \"result2\",\n" +
              "            \"plugin_name\": \"sql\",\n" +
              "            \"table_name\": \"user_view\",\n" +
              "            \"sql\": \"select * from user_view where dt = '20190318'\"\n" +
              "        }\n" +
              "    ],\n" +
              "    \"output\": [\n" +
              "        {\n" +
              "            \"source_table_name\": \"result1\",\n" +
              "            \"plugin_name\": \"stdout\"\n" +
              "        },\n" +
              "        {\n" +
              "            \"plugin_name\": \"stdout\"\n" +
              "        }\n" +
              "    ]\n" +
              "}";

      JSONObject jsonObj = JSON.parseObject(jsonDoc);

      System.out.println(jsonObj);

      Config config1 = ConfigFactory.parseMap(jsonObj);


      config = config.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
      .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

    ConfigRenderOptions renderOptions = ConfigRenderOptions.defaults().setComments(false).setOriginComments(false);
    //System.out.println(config.root().render(renderOptions));

      Config config2 = config1.resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
              .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

      System.out.println(config2.root().render(renderOptions));

      System.out.println(config.root().render(renderOptions));

  }

  // get file from classpath, resources folder
  private File getFileFromResources(String fileName) {

    ClassLoader classLoader = getClass().getClassLoader();

    URL resource = classLoader.getResource(fileName);
    if (resource == null) {
      throw new IllegalArgumentException("file is not found!");
    } else {
      return new File(resource.getFile());
    }

  }
}
