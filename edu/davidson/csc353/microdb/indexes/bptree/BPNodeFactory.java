package edu.davidson.csc353.microdb.indexes.bptree;

import java.io.FileNotFoundException;
import java.io.IOException;

import java.io.RandomAccessFile;

import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Function;

import javax.tools.Diagnostic;

import edu.davidson.csc353.microdb.files.Block;
import edu.davidson.csc353.microdb.utils.DecentPQ;

public class BPNodeFactory<K extends Comparable<K>, V> {
	public static final int DISK_SIZE = 512;

	public static final int CAPACITY = 15;

	private String indexName;

	private Function<String, K> loadKey;
	private Function<String, V> loadValue;

	private int numberNodes;

	private RandomAccessFile relationFile;
	private FileChannel relationChannel;

	// TODO You should change the type of this nodeMap
	// CHANGE! changed this from type Int, BPNode to Int, NodeTimestamp based on MicroDB
	private HashMap<Integer, NodeTimestamp> nodeMap;
	private DecentPQ<NodeTimestamp> nodePQ;

	private class NodeTimestamp implements Comparable<NodeTimestamp> {
		public BPNode<K, V> node;
		public long lastUsed;

		public NodeTimestamp(BPNode<K, V> node, long lastUsed) {
			this.node = node;
			this.lastUsed = lastUsed;
		}

		public int compareTo(NodeTimestamp other) {
			return (int) (lastUsed - other.lastUsed);
		}
	}

	/**
	 * Creates a new NodeFactory object, which will operate a buffer manager for
	 * the nodes of a B+Tree.
	 * 
	 * @param indexName The name of the index stored on disk.
	 */
	public BPNodeFactory(String indexName, Function<String, K> loadKey, Function<String, V> loadValue) {
		try {
			this.indexName = indexName;
			this.loadKey = loadKey;
			this.loadValue = loadValue;

			Files.delete(Paths.get(indexName + ".db"));

			relationFile = new RandomAccessFile(indexName + ".db", "rws");
			relationChannel = relationFile.getChannel();

			numberNodes = 0;

			nodeMap = new HashMap<>();
			nodePQ = new DecentPQ<>();
		} catch (FileNotFoundException exception) {
			// Ignore: a new file has been created
		} catch (IOException exception) {
			throw new RuntimeException("Error accessing " + indexName);
		}
	}

	/**
	 * Creates a B+Tree node.
	 * 
	 * @param leaf Flag indicating whether the node is a leaf (true) or internal
	 *             node (false)
	 * 
	 * @return A new B+Tree node.
	 */
	public BPNode<K, V> create(boolean leaf) {
		BPNode<K, V> created = new BPNode<K, V>(leaf);
		created.number = numberNodes;
		// CHANGE! I changed the name of the nodeTimestamp var to nodeTimestamp bc it was clearer to me, we can change back tho!
		// Also, I put nodeTimestamp into nodeMap instead of created!
		NodeTimestamp nodeTimestamp = new NodeTimestamp(created, System.nanoTime());
		if (nodeMap.size() == BPNodeFactory.CAPACITY) {
			evict();
		}
		nodeMap.put(created.number, nodeTimestamp);
		nodePQ.add(nodeTimestamp);
		numberNodes++;

		return created;
	}

	/**
	 * Saves a node into disk.
	 * 
	 * @param node Node to be saved into disk.
	 */
	public void save(BPNode<K, V> node) {
		writeNode(node);
	}

	/**
	 * Reads a node from the disk.
	 * 
	 * @param nodeNumber Number of the node read.
	 * 
	 * @return Node read from the disk that has the provided number.
	 */
	private BPNode<K, V> readNode(int nodeNumber) {
		// TODO
		// index into file chunk from nodeNumber * Disk size.
		// read from disk into a ByteBuffer
		// read in information from the ByteBuffer (im guessing into varables?)
		// create a new node. "in memory"
		// call the new node.load()
		long diskRead = nodeNumber * DISK_SIZE;
		ByteBuffer nodeObj = ByteBuffer.allocate(DISK_SIZE);
		try {
			relationChannel.read(nodeObj, diskRead);
			// CHANGE! I added all this stuff based on MicroDB.
			// idea: create a new node, load the nodeObj from buffer into this new node
			// QUESTION: is creating a new node this way considered creating it "in memory" (pdf specifies in memory)??
			BPNode<K, V> newNode = new BPNode<>(false);
			// QUESTION: what do we pass in here – these functions, or do we pass in loadKey and loadValue??
			newNode.load(nodeObj, loadKey, loadValue);
			return newNode;
		} catch (IOException e) {
			// QUESTION: what should we do for run time exceptions?
			throw new RuntimeException("Error accessing node with number" + nodeNumber);
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	/**
	 * Writes a node into disk.
	 * 
	 * @param node Node to be saved into disk.
	 */
	private void writeNode(BPNode<K, V> node) {
		// CHANGE! i changed the name from nodeSave to buffer
		ByteBuffer buffer = ByteBuffer.allocate(DISK_SIZE);
		node.save(buffer);
		long diskPos = node.number * DISK_SIZE;
		try {
			// QUESTION: writeBlock() has blockBuffer.rewind() here. should we have:
			// buffer.rewind();
			relationChannel.write(buffer, diskPos);
		} catch (IOException e) {
			// QUESTION: again, not sure what to do for the exceptions. this is kinda based off of microDB
			throw new RuntimeException("Error accessing " + node.number);
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
	}

	/**
	 * Evicts the last recently used node back into disk.
	 */
	private void evict() {
		// CHANGE! just used most of the stuff you had. 
		// Only changed it to remove(oldest.node.number) instead of remove(oldest.getNode())
		// im not sure which we want.
		NodeTimestamp oldest = nodePQ.removeMin(); // removing the smallest
		// QUESTION: How should we get the node? Can we do it this way, or, nodeMap.remove(oldest.getNode()); ?
		nodeMap.remove(oldest.node.number);
		writeNode(oldest.node);
		// question: doesn't the information never really leave the disk? wym back into
		// disk?
	}

	/**
	 * Returns the node associated with a particular number.
	 * 
	 * @param number The number to be converted to node (loading it from disk, if
	 *               necessary).
	 * 
	 * @return The node associated with the provided number.
	 */
	public BPNode<K, V> getNode(int number) {
		// CHANGE! basing it off of MicroDB files.
		// if nodeMap has the node number, then get the associated timestamp,
		// update the timestamp for that node, increase its key in the PQ,
		// and return this node
		if (nodeMap.containsKey(number)) {
			NodeTimestamp nodeTimestamp = nodeMap.get(number);
			nodeTimestamp.lastUsed = System.nanoTime();
			nodePQ.increaseKey(nodeTimestamp);
			return nodeTimestamp.node;
		}
		// wasn't quite as sure what to do here.
		// before adding an entry to your map – do the if statement
		// check if you've just gone over capacity – if you have, call evict
		else {
			BPNode<K,V> loadedNode = readNode(number);
			NodeTimestamp nodeTimestamp = new NodeTimestamp(loadedNode, System.nanoTime());
			if (nodeMap.size() == BPNodeFactory.CAPACITY) {
				evict();
			}
			nodeMap.put(loadedNode.number, nodeTimestamp);
			nodePQ.add(nodeTimestamp);
			// QUESTION: is this how we should access/return the node? Should we return loadedNode instead?
			return nodeTimestamp.node;
		}

		// NodeTimestamp nodeTimestamp = nodeMap.get(number);
		// nodeTimestamp.lastUsed = System.nanoTime();

		// boolean toFind = true;
		// if (toFind) {
		// 	returnNode = nodeMap.get(number);
		// } else {
			// long diskRead = number * DISK_SIZE;
			// ByteBuffer nodeObj = ByteBuffer.allocate(DISK_SIZE);
			// try {
			// relationChannel.read(nodeObj, diskRead);
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
		// 	returnNode = readNode(number);
		// }
		// // check if in memory
		// // look in disk if not
		// // ByteBuffer looking = ByteBuffer.
		// return returnNode;
	}
	// create evictAll()
		// while map has any values – evict
		// then, go into main tester
		// make an operation – as for value associated with the key
		// call this wipeout function
		// force all nodes to be read back from disk
		// when you evict the node, are you evicting the right one? –
		// when you call evict, call remove min in PQ
		// make sure every time you return a node, update the timestamp & update the PQ
	public void evictAll() {
		while (!nodeMap.isEmpty()) {
			evict();
		}
	}
}
