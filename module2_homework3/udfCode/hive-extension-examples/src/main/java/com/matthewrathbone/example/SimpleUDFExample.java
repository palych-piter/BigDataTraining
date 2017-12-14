package com.matthewrathbone.example;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

@Description(
  name="SimpleUDFExample",
  value="returns an array with parsed values of UserAgent log field",
  extended="SELECT userAgentUDF(UserAgent) from logs;"
  )
public class SimpleUDFExample extends UDF {


  public ArrayList<String> evaluate(Text input) {
    if(input == null) return null;

    ArrayList<String> userAgentArray = new ArrayList<String>();

    UserAgent userAgent = UserAgent.parseUserAgentString(input.toString());

    userAgentArray.add(userAgent.getOperatingSystem().getDeviceType().toString());
    userAgentArray.add(userAgent.getBrowser().toString());
    userAgentArray.add(userAgent.getOperatingSystem().toString());

    return userAgentArray;
  }

}