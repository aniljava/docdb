package docdb;

import java.io.Closeable;
import java.util.Collection;

public interface DB extends Closeable{

	/** Underlying KV Store **/
	public KV kv();

	/** Document **/
	public <U> U get(String bucket, Class<U> valueType, Object... criteria);
	public <U> Collection<U> list(String bucket, Class<U> valueType, Object... criteria);
	public Object save(String bucket, Object key, Object data);
	public void delete(String bucket, Object key);
	public void updateBucketFields(String bucket, String... fields);
	
	/** Index or Set**/	
	public void ensureIndex(String bucket, String... fields);
	public void unEnsureIndex(String bucket, String... fields);
	public Collection<String> listIndexValues(String... key);
	public void index(String term, Object... value);
	public void unIndex(String term, Object... value);
	
	/** List **/
	public void prepend(String listName, Object... data);
	public void append(String listName, Object... data);
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
