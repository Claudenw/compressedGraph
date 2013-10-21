package org.xenei.compressedgraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.graph.Node;

public class NodeMap {
	private Map<Node,Integer> map;
	private List<Node> lst;
	
	public NodeMap()
	{
		map = new HashMap<Node,Integer>();
		lst = new ArrayList<Node>();
	}
	
	public synchronized int get(Node n)
	{
		if (n==null || n == Node.ANY)
			return BitConstants.WILD;
		
		Integer i = map.get(n);
		if (i == null)
		{
			return add(n);
		}
		return i;
	}
	
	public Node get(int idx)
	{
		return lst.get(idx);
	}
	
	private synchronized int add(Node n)
	{
		int i = lst.size();
		lst.add(n);
		map.put(n, i);
		if (map.size() != lst.size())
		{
			throw new IllegalStateException( "lists out of order");
		}
		return i;
	}
}
