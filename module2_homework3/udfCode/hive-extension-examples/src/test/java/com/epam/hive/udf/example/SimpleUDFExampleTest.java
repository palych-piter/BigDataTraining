package com.epam.hive.udf.example;

import junit.framework.Assert;

import org.junit.Test;

import java.util.ArrayList;

public class SimpleUDFExampleTest {
  
  @Test
  public void testUDF() {
    SimpleUDFExample example = new SimpleUDFExample();

    ArrayList<String> testArray = new ArrayList<String>();
    testArray = example.evaluate("Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)");
    String outputParsedValues = testArray.get(0) + testArray.get(1) + testArray.get(2);
    Assert.assertEquals("COMPUTERIE6WINDOWS_XP", outputParsedValues);

  }
  
  @Test
  public void testUDFNullCheck() {
    SimpleUDFExample example = new SimpleUDFExample();
    Assert.assertNull(example.evaluate(null));
  }
}