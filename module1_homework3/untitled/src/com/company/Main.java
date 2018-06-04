package com.company;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) {
        //the pattern to check if the string has the symbols
        Pattern regex = Pattern.compile("/-.*?/-");
        //matcher to find if there is any special character in string
        Matcher matcher = regex.matcher("-1-");

    }
}
