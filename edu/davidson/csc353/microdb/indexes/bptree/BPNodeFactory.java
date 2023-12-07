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
	private HashMap<Integer, BPNode<K, V>> nodeMap;
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
		NodeTimestamp newOne = new NodeTimestamp(created, System.nanoTime());
		nodeMap.put(created.number, created);
		nodePQ.add(newOne);
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
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Writes a node into disk.
	 * 
	 * @param node Node to be saved into disk.
	 */
	private void writeNode(BPNode<K, V> node) {
		ByteBuffer nodeSave = ByteBuffer.allocate(DISK_SIZE);
		node.save(nodeSave);
		long diskPos = node.number * DISK_SIZE;
		try {
			relationChannel.write(nodeSave, diskPos);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Evicts the last recently used node back into disk.
	 */
	private void evict() {
		// TODO
		// NodeTimestamp oldest = nodePQ.removeMin(); // removing the smallest
		// nodeMap.remove(oldest.node.getNode());
		// relation.writeNode(oldest.node);
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
		// TODO
		BPNode<K, V> returnNode;
		boolean toFind = true;
		if (toFind) {
			returnNode = nodeMap.get(number);
		} else {
			// long diskRead = number * DISK_SIZE;
			// ByteBuffer nodeObj = ByteBuffer.allocate(DISK_SIZE);
			// try {
			// relationChannel.read(nodeObj, diskRead);
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			returnNode = readNode(number);
		}
		// check if in memory
		// look in disk if not
		// ByteBuffer looking = ByteBuffer.
		return returnNode;
	}
}
