package com.stat;

import java.util.Comparator;
import java.util.HashMap;

public class CharValueComparatorDesc implements Comparator<Character>{
	HashMap<Character,Integer> map = new HashMap<Character,Integer>();
	
	public CharValueComparatorDesc(HashMap<Character,Integer>map)
	{
		this.map.putAll(map);
	}

	public int compare(Character c1, Character c2) {
		if(map.get(c1)>=map.get(c2))
			return -1;
		else
			return 1;
	}

}
