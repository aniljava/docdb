package docdb;

import java.io.Closeable;
import java.util.Collection;

/**
 * This interface is of client's interest. It lists all the operations to be guaranteed by the underlying implementation.
 */
public interface DB extends Closeable{

	/** Underlying KV Store **/
	public KV kv();
	
	/** General KV Shorthands **/
	public String get(String key);	
	public void set(String key, String value);
	public void remove(String key);
	
	
	

	/** Document **/
	public <U> U get(String bucket, Class<U> valueType, Object... criteria);
	public <U> Collection<U> list(String bucket, Class<U> valueType, Object... criteria);
	public Object save(String bucket, Object key, Object data);
	public void delete(String bucket, Object key);
	public void updateBucketFields(String bucket, String... fields);
	public Collection<String> getFieldNames(String bucket);
	
	/** Index or Set**/	
	public void ensureIndex(String bucket, String... fields);
	public void unEnsureIndex(String bucket, String... fields);
	public Collection<String> listIndexValues(String... key);
	public void index(String term, Object... value);
	public void unIndex(String term, Object... value);
	public boolean isIndexed(String term, String docid);
	public void addToIndices(Object value, String... terms);
	public void removeFromIndices(Object value, String... terms);
	
	/** List **/
	public void prepend(String listName, Object... data);
	public void append(String listName, Object... data);	
	public void prependToLists(Object data, String... lists);
	public void appendToLists(Object data, String... lists);	
	public long getListSize(String listName);
	public long getListStartIndex(String listName);
	public void removeFromList(String listName, long index);
	public String getFromList(String listName, long index);
	public void insertIntoList(String listName, long index, Object value);
	public Collection<String> getListRange(String listName, long start, long length);
	
	
	/** Counters **/
	public long counter(String name);
	public void setCounter(String name, long value);	
	public void removeCounter(String name);
	public long peekConter(String name);
}
