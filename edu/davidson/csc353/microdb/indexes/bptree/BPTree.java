package edu.davidson.csc353.microdb.indexes.bptree;

import java.nio.ByteBuffer;

import java.util.function.Function;

/**
 * B+Tree implementation.
 *
 * @param <K> Type of the keys in the B+Tree node.
 * @param <V> type of the values associated with the keys in the B+Tree node.
 */
public class BPTree<K extends Comparable<K>, V> {
	private BPNodeFactory<K,V> nodeFactory;

	private int rootNumber;

	public BPTree(Function<String, K> loadKey, Function<String, V> loadValue) {
		nodeFactory = new BPNodeFactory<>("test-index", loadKey, loadValue);

		// The node will be garbage-collected, but retrievable from disk
		BPNode<K,V> rootNode = nodeFactory.create(true);
		//nodeFactory.save(rootNode);

		rootNumber = rootNode.number;
	}

	/**
	 * Inserts the key-value pair into the B+Tree.
	 * 
	 * @param key The key.
	 * @param value The value associated with the key.
	 */
	public void insert(K key, V value) {
		System.out.println("Inserting " + key);

		// if tree is empty { create an empty leaf node L, which is also the root }
		// else {...... the rest of this stuff should go in here 

		BPNode<K,V> insertPlace = find(nodeFactory.getNode(rootNumber), key);
		// if insertPlace has 2+ more spaces in keys array/values array (meaning it is NOT at the maximum capacity of keys)
			// insert the key-value pair within this leaf
		// else 
			// insert the key-value pair as the last entry within this leaf (?)
			// then call splitLeaf()

		// TODO ...

		// insertPlace = location to insert new key/value
		// insertPlace = find(node, key)
		// put (k, v) into insertPlace
		// if the node insertPlace has overflown after inserting - which we know if we just filled the last entry in the .keys or .values arrays
		// since the arrays contain one more entry than the maximum allowed (so we can add the key-value FIRST and THEN split the node)
			// call splitLeaf()

		// }
		
		// Need to call insertOnParent after performing a leaf node split
	}

	/**
	 * Insert on parent node a divider key after a child has been splitted.
	 * 
	 * @param left Left B+Tree node after a split has been made.
	 * @param key Divider key (according to the description in {@link SplitResult}.
	 * @param right Right B+Tree node after a split has been made.
	 */
	private void insertOnParent(BPNode<K,V> left, K key, BPNode<K,V> right) {
		// TODO ...
		// call this if you split the node
		// required to insert the first key of the right split into the parent node
					
		// Need to keep calling insertOnParent after performing an internal node split
	}

	/**
	 * Returns a value associated with a particular key.
	 * 
	 * @param key The key.
	 * 
	 * @return The value associated with the provided key.
	 */
	public V get(K key) {
		// TODO ...
		// check if nF.getNode works. if not, use create()
		BPNode<K, V> leafToSearch = find(nodeFactory.getNode(rootNumber), key);
		// should this be a binary search?
		for (int i = 0; i < leafToSearch.keys.size(); i++) {
			if (leafToSearch.getKey(i) == key) {
				return leafToSearch.getValue(i);
			}
		}
		return null;
	}

	/**
	 * Returns the leaf node where we should look for the provided key.
	 * 
	 * @param node The current node in the recursive search procedure.
	 * @param key The key being searched.
	 * 
	 * @return The leaf node where we should look for the provided key.
	 */
	private BPNode<K,V> find(BPNode<K,V> node, K key) {
		if(node.isLeaf()) {
			return node;
		}
		else {
		// TODO ...
		for (int i = 0; i < node.keys.size(); i++) {
			// if it's less than the key at current position, 
			// go to the child node pointed to before it
			if (BPTree.less(key, node.getKey(i))) {
				return find(nodeFactory.getNode(node.getChild(0)), key);
			}
		}
		return null;
	}
}

	/**
	 * Helper method: returns true if k1 < k2.
	 * 
	 * @param k1 The first key k1.
	 * @param k2 The second key k2.
	 * 
	 * @return True iff k1 < k2.
	 */
	public static <K extends Comparable<K>> boolean less(K k1, K k2) {
		return k1.compareTo(k2) < 0;
	}

	/**
	 * Helper method: returns true if k1 == k2.
	 * 
	 * @param k1 The first key k1.
	 * @param k2 The second key k2.
	 * 
	 * @return True iff k1 == k2.
	 */
	public static <K extends Comparable<K>> boolean equal(K k1, K k2) {
		return k1.compareTo(k2) == 0;
	}

	/**
	 * Helper method: returns true if k1 > k2.
	 * 
	 * @param k1 The first key k1.
	 * @param k2 The second key k2.
	 * 
	 * @return True iff k1 > k2.
	 */
	public static <K extends Comparable<K>> boolean more(K k1, K k2) {
		return k1.compareTo(k2) > 0;
	}

	/**
	 * Main method. Creates a test index and add some key-value pairs.
	 * 
	 * @param arguments None.
	 */
	public static void main(String[] arguments) {
		BPTree<String, Integer> testIndex = new BPTree<>(k -> k, s -> Integer.parseInt(s));

		testIndex.insert("i", 9);
		testIndex.insert("l", 12);
		testIndex.insert("g", 7);
		testIndex.insert("o", 15);
		testIndex.insert("d", 4); // Generates leaf node split
		testIndex.insert("f", 6);
		testIndex.insert("a", 1);
		testIndex.insert("c", 3); // Generates leaf node split
		testIndex.insert("p", 16);
		testIndex.insert("k", 11); // Generates leaf node split
		testIndex.insert("b", 2);
		testIndex.insert("e", 5);
		testIndex.insert("n", 14);
		testIndex.insert("h", 8); // Generates leaf node split
		testIndex.insert("j", 10);
		testIndex.insert("m", 13); // Generates an internal node split

		System.out.println(testIndex.get("g")); // Should print 7
		System.out.println(testIndex.get("o")); // Should print 15
		System.out.println(testIndex.get("a")); // Should print 1
		System.out.println(testIndex.get("c")); // Should print 3
		System.out.println(testIndex.get("x")); // Should print null

		BPNode<String, Integer> node1 = testIndex.nodeFactory.getNode(testIndex.rootNumber);
		BPNode<String, Integer> node2 = testIndex.find(node1, "a");

		ByteBuffer buffer1 = ByteBuffer.allocate(512);
		ByteBuffer buffer2 = ByteBuffer.allocate(512);

		node1.save(buffer1);
		node2.save(buffer2);

		BPNode<String, Integer> newNode1 = new BPNode<>(false);
		BPNode<String, Integer> newNode2 = new BPNode<>(true);

		newNode1.load(buffer1, k -> k, s -> Integer.parseInt(s));
		newNode2.load(buffer2, k -> k, s -> Integer.parseInt(s));

		System.out.println("Original root: " + node1 + ", parent = " + node1.parent + ", next = " + node1.next + ", number = " + newNode1.number);
		System.out.println("Loaded root:   " + newNode1 + ", parent = " + newNode1.parent + ", next = " + newNode1.next + ", number = " + newNode1.number);

		System.out.println("Original leaf: " + node2 + ", parent = " + node2.parent + ", next = " + node2.next + ", number = " + newNode2.number);
		System.out.println("Loaded leaf:   " + newNode2 + ", parent = " + newNode2.parent + ", next = " + newNode2.next + ", number = " + newNode2.number);
	}
}
