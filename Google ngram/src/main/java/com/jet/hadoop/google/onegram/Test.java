package com.jet.hadoop.google.onegram;

import java.util.Arrays;

public class Test {

	public static void main(String[] args) {
		String line = "X'rays	1952	25	2";
		String[] arr = line.trim().split("\t");
		System.out.println(arr.length);
		System.out.println(Arrays.toString(arr));
	}

}
