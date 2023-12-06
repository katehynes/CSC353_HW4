package edu.davidson.csc353.microdb.indexes.bptree;

import java.nio.ByteBuffer;

import java.util.ArrayList;

import java.util.function.Function;

import java.nio.charset.Charset;

/**
 * B+Tree node (internal or leaf).
 * 
 * @param <K> Type of the keys in the B+Tree node.
 * @param <V> type of the values associated with the keys in the B+Tree node.
 */
public class BPNode<K extends Comparable<K>, V> {
	// Maximum number of children in an internal node (fanout)
	// A leaf node can have at most SIZE - 1 key-value pairs
	public static int SIZE = 5;

	// Flag indicating if a leaf or not
	boolean leaf;

	// Reference to the parent
	public int parent;

	// Keys:
	// - in the leaf node, they are the associated with values
	// - in internal nodes, they separate the pointers to the children
	public ArrayList<K> keys;

	// (Leaf node only) Values associated with each key
	public ArrayList<V> values;
	// (Leaf node only) Next leaf node
	public int next;

	// (Internal node only) Children of the internal node
	public ArrayList<Integer> children;

	// Node number
	public int number;

	/**
	 * Constructor.
	 * 
	 * @param leaf Flag indicating whether the node is a leaf (true) or internal
	 *             node (false)
	 */
	public BPNode(boolean leaf) {
		this.leaf = leaf;

		this.parent = -1;
		this.next = -1;

		// Contains one more than you allow, because you may want to add, and then split
		this.keys = new ArrayList<>(SIZE);

		// Contains one more than you allow, because you may want to add, and then split
		this.values = new ArrayList<>(SIZE);

		// Contains one more than you allow, because you may want to add, and then split
		this.children = new ArrayList<>(SIZE + 1);
	}

	/**
	 * Returns true if the B+Tree node is a leaf node.
	 * 
	 * @return True if the B+Tree node is a leaf node.
	 */
	public boolean isLeaf() {
		return leaf;
	}

	/**
	 * Returns the key in a given position.
	 * 
	 * @param index Position of the key.
	 * 
	 * @return Key in the specified position.
	 */
	public K getKey(int index) {
		return keys.get(index);
	}

	/**
	 * Returns the child in a given position.
	 * This can only be called for internal nodes.
	 * 
	 * @param index Position of the child.
	 * 
	 * @return Child in the specified position.
	 */
	public int getChild(int index) {
		return children.get(index);
	}

	/**
	 * Returns the value in a given position.
	 * This can only be called for leaf nodes.
	 * 
	 * @param index Position of the value.
	 * 
	 * @return Value in the specified position.
	 */
	public V getValue(int index) {
		return values.get(index);
	}

	/**
	 * (Leaf node only) Inserts a (K,P) pair in the appropriate position in the
	 * node.
	 * 
	 * @param key   The key.
	 * @param value The value associated with the key.
	 */
	public void insertValue(K key, V value) {
		int i;

		for (i = 0; i < keys.size(); i++) {
			if (BPTree.more(keys.get(i), key)) {
				break;
			}
		}

		keys.add(i, key);
		values.add(i, value);
	}

	/**
	 * (Internal node only) Inserts a (K,P) pair in the appropriate position in the
	 * node.
	 * 
	 * NOTE: The pointer associated with key at position i is actually located at
	 * position i+1
	 * due to the first pointer.
	 * 
	 * @param key         The key.
	 * @param value       The child node reference that should FOLLOW the key.
	 * @param nodeFactory Factory that generates new nodes.
	 */
	public void insertChild(K key, int childNumber, BPNodeFactory<K, V> nodeFactory) {
		int i;

		for (i = 0; i < keys.size(); i++) {
			if (BPTree.more(keys.get(i), key)) {
				break;
			}
		}

		keys.add(i, key);
		children.add(i + 1, childNumber);

		BPNode<K, V> child = nodeFactory.getNode(childNumber);

		child.parent = this.number;
	}

	/**
	 * Splits an overflowed leaf node into a {@link SplitResult} object,
	 * that contains:
	 * - A reference to the left B+Tree node;
	 * - A reference to the right B+Tree node;
	 * - The first key in the right B+Tree node (the "divider Key" in the
	 * {@link SplitResult} object).
	 * 
	 * The method also makes the left node point to the right node in the "next"
	 * attribute.
	 * 
	 * @param nodeFactory Factory that generates new nodes.
	 * 
	 * @return A new {@link SplitResult} following the specifications above.
	 */
	public SplitResult<K, V> splitLeaf(BPNodeFactory<K, V> nodeFactory) {
		SplitResult<K, V> result = new SplitResult<>();

		result.left = this;
		result.right = nodeFactory.create(true);

		int SPLIT_INDEX = SIZE / 2;
		result.dividerKey = result.left.getKey(SPLIT_INDEX);
		for (int i = SPLIT_INDEX; i < SIZE; i++) {
			result.right.insertValue(result.left.getKey(i), result.left.getValue(i));
		}
		// go backwards and remove them after copying
		for (int i = SIZE - 1; i >= SPLIT_INDEX; i--) {
			result.left.keys.remove(result.left.getKey(i));
		}

		return result;
	}

	/**
	 * Splits an overflowed leaf internal into a {@link SplitResult} object,
	 * that contains:
	 * - A reference to the left B+Tree node;
	 * - A reference to the right B+Tree node;
	 * - The divider key that will be subsequently be inserted in the parent node
	 * (stored in the "divider Key" in the {@link SplitResult} object).
	 * 
	 * @param nodeFactory Factory that generates new nodes.
	 * 
	 * @return A new {@link SplitResult} following the specifications above.
	 */
	public SplitResult<K, V> splitInternal(BPNodeFactory<K, V> nodeFactory) {
		SplitResult<K, V> result = new SplitResult<>();

		result.left = this;
		result.right = nodeFactory.create(false);

		int SPLIT_INDEX = SIZE / 2;
		result.dividerKey = result.left.getKey(SPLIT_INDEX);
		// add keys
		for (int i = SPLIT_INDEX + 1; i < SIZE; i++) {
			result.right.keys.add(result.left.getKey(i));
		}
		// add children
		for (int i = SPLIT_INDEX + 1; i <= SIZE; i++) {
			result.right.children.add(result.left.children.get(i));
		}
		// go backwards and remove them after copying
		for (int i = SIZE - 1; i >= SPLIT_INDEX; i--) {
			result.left.keys.remove(result.left.getKey(i));
		}

		return result;

	}

	/**
	 * Returns a string representation of the B+Tree node.
	 */
	public String toString() {
		String result = "[(#: " + number + ") ";

		// If leaf, print [k1 v1 k2 v2 ... kn vn]
		if (isLeaf()) {
			for (int i = 0; i < keys.size() - 1; i++) {
				result += keys.get(i);
				result += " ";
				result += values.get(i);
				result += " ";
			}

			if (keys.size() > 0) {
				result += keys.get(keys.size() - 1);
				result += " ";
				result += values.get(keys.size() - 1);
			}
		}
		// If internal, print [<c0> k1 <c1> k2 <c2> ... kn <cn>], where
		else {
			for (int i = 0; i < keys.size(); i++) {
				result += "<" + children.get(i) + ">";
				result += " ";
				result += keys.get(i);
				result += " ";
			}

			if (keys.size() > 0) {
				result += "<" + children.get(keys.size()) + ">";
			}
		}

		result += "]";

		return result;
	}

	/**
	 * Loads information from a buffer of 512 bytes (loaded from disk)
	 * into the memory representation of a B+Tree node.
	 * 
	 * @param buffer    A byte buffer of 512 bytes containing the node
	 *                  representation from disk.
	 * @param loadKey   A function that converts string representations into keys of
	 *                  type K
	 * @param loadValue A function that converts string representations into values
	 *                  of type V
	 */
	public void load(ByteBuffer buffer, Function<String, K> loadKey, Function<String, V> loadValue) {
		buffer.rewind();

		if (buffer.getInt() == 0) {
			this.leaf = false;
		} else {
			this.leaf = true;
		}
		this.parent = buffer.getInt();
		// for keys
		String keyByteString = "";
		keyByteString = new String(buffer.array());
		// is this the right way to convert from byte back to string?? getoing weird
		// wutput (diamond question marks) do we get it to split around the chars we
		// want
		// also it's returning " a$b$c$ 1$2$3$ " as one string... don'tt we want it to
		// be 2 strings
		String[] keyParts = keyByteString.split("$");
		for (int i = 0; i < keyParts.length; i++) {
			K key = loadKey.apply(keyParts[i]);
			this.keys.add(key);
			// read length then byte [] b = newBythe[legnth]
			// buffer.get(b)
			// new string(byte array)
		}
		if (this.leaf) {
			// for values
			String valueByteString = "";
			valueByteString = new String(buffer.array()); // should use default charset to convert into string
			String[] valueParts = valueByteString.split("$"); // where is all the null characters coming from?
			for (int i = 0; i < valueParts.length; i++) {
				V value = loadValue.apply(valueParts[i]);
				this.values.add(value);
			}
			this.next = buffer.getInt();
		} else {
			for (int i = 0; i < buffer.getInt(); i++) {
				this.children.add(buffer.getInt());
			}
		}
		this.number = buffer.getInt();
		// TODO: Load from disk (that is, from the buffer), create your own file format
		// The getInt() and putInt() functions should be very helpful

	}

	// want BPnf to take the node & the buffer and write to the disk
	// offset = 2 * disk size / size of the buffer
	/*
	 * go to 3x the disk size and call read
	 * 
	 * put int to put info into the buffer – if leaf or not,
	 * keys and values if you're leaf,
	 * keys and children if you're non-leaf
	 * 0 |
	 * goover array of keys, and do .toString but add seperators
	 * take that string and dump it into the buffer
	 * when you load it, parse through the seperators and put the values back in
	 * load function splits string & reads it into array
	 * for internals, you have one more child than keys. for leafs, # keys = #
	 * values
	 * use the functions to load from a key to a key type
	 * you don't need to loadChildren b/c they're always integers
	 * 
	 * whenever you get the string rep of a node to dump into the buffer, do a
	 * putInt with length & dump the bytes
	 * read the number & create new byte array with that number
	 * buffer.get -> pass in that buffer, reads in as many bytes as the length of
	 * the buffer = the length of the string
	 * 
	 * 
	 * save – write the node you're kicking out to the disk/buffer
	 * allocate a buffer of disc size, save, and fire channel.write
	 * 
	 * load - create a new node with nothing inside,
	 * and put the buffer representation of that new node into the node
	 */

	/**
	 * Save the memory representation of the B+Tree node
	 * into a buffer of 512 bytes (to be stored on disk).
	 * 
	 * @param buffer
	 */
	public void save(ByteBuffer buffer) {
		buffer.rewind();

		int leafInt = 0;
		if (this.leaf) {
			leafInt = 1;
		}
		buffer.putInt(leafInt);
		buffer.putInt(this.parent);
		// size number
		buffer.putInt(this.keys.size());
		String keyString = "";
		for (int i = 0; i < this.keys.size(); i++) {
			keyString = keyString + this.keys.get(i) + "$";
		}
		byte[] stringKBytes = keyString.getBytes();
		// put int bytes length
		buffer.put(stringKBytes);
		// size number
		if (leafInt == 1) {
			buffer.putInt(this.keys.size());
			String valString = "";
			for (int i = 0; i < this.keys.size(); i++) {
				valString = valString + this.values.get(i) + "$";
			}
			byte[] stringVBytes = valString.getBytes();
			buffer.put(stringVBytes);
			buffer.putInt(this.next);
		} else {
			buffer.putInt(this.children.size());
			for (int i = 0; i < this.children.size(); i++) {
				buffer.putInt(this.children.get(i));
			}
		}
		buffer.putInt(this.number);

		// TODO: Save to disk (that is, to the buffer), create your own file format
		// The getInt() and putInt() functions should be very helpful
		// To save a string, generate it in memory, then use getBytes() and use the
		// put() function in the buffer.
	}
}
