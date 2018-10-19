package com.company;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) {

        //Handle integer arrays
        int[] array = new int[]{1, 2};
        //int[] sum = Arrays.stream(array).map(i-> i * 2).toArray();
        int[] sum = Arrays.stream(array).map(i -> {
                    System.out.printf(String.valueOf(i * 2));
                    return i * 2;
                }
        ).toArray();

        //Handle string arrays
        String[] arrayString = new String[]{"Go","Home"};
//        Arrays.stream(arrayString).forEach( s -> {
//             s.toUpperCase();
//        });


        String converted = Arrays.stream(arrayString).map((String s)-> s.toUpperCase());


//        Arrays.stream(sum).forEach(s -> System.out.printf(String.valueOf(s)));

    }
}