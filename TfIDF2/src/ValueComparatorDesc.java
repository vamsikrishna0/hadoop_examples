

import java.util.Comparator;
import java.util.HashMap;

public class ValueComparatorDesc implements Comparator<String>
{
	HashMap<String,Integer> map = new HashMap<String,Integer>();
	
	public ValueComparatorDesc(HashMap<String,Integer>map)
	{
		this.map.putAll(map);
	}

	public int compare(String s1, String s2) {
		if(map.get(s1)>=map.get(s2))
			return -1;
		else
			return 1;
	}
	
}
