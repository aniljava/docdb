package docdb;

import java.io.UnsupportedEncodingException;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.codehaus.jackson.map.ObjectMapper;

public class DocDB implements DB {

	public KV	kv;

	public KV kv() {
		return kv;
	}

	public DocDB(KV db) {
		this.kv = db;
		loadSchema();
	}

	private void loadSchema() {
		byte[] json = kv.get(s2a("schema"));
		@SuppressWarnings("unchecked")
		Map<String, Map<String, Map<String, Map<String, Object>>>> schema = decode(json, Map.class);
		if (schema != null) {
			this.schema = schema;
		}
	}

	private Map<String, Map<String, Map<String, Map<String, Object>>>>	schema	= new HashMap<String, Map<String, Map<String, Map<String, Object>>>>();

	public long counter(String name) {
		if (name == null)
			name = "__default";
		long current = longOrElse(kv.get(s2a("counter:" + name)), 0);
		kv.set(s2a("counter:" + name), s2a("" + (current + 1)));
		return current;
	}

	public void setCounter(String name, long value) {
		kv.set(s2a("counter:" + name), s2a("" + (value)));
	}

	public void removeCounter(String name) {
		kv.remove(s2a("counter:" + name));
	}

	public long peekConter(String name) {
		if (name == null)
			name = "__default";
		long current = longOrElse(kv.get(s2a("counter:" + name)), 0);
		return current;
	}

	public Collection<String> getListRange(final String listName, final long start, final long length) {

		class ListRangeCollection extends AbstractCollection<String> implements Iterator<String> {

			String		next;
			long		current		= start;

			final long	listStart	= getListStartIndex(listName);
			final long	listSize	= getListSize(listName);

			public Iterator<String> iterator() {
				return this;
			}

			public int size() {
				return (int) listSize;
			}

			public boolean hasNext() {
				if (current < listStart || current > listStart + listSize)
					return false;

				next = getFromList(listName, current);
				current = current + 1;
				if (next == null)
					return hasNext();
				else
					return true;
			}

			public String next() {
				return next;
			}

			public void remove() {
				throw new RuntimeException("Not implemented");

			}
		}
		return new ListRangeCollection();
	}

	public void prepend(final String listName, final Object... data) {
		if (data == null || data.length == 0)
			return;

		if (data.length == 1) {
			final long size = getListSize(listName);
			final long start = getListStartIndex(listName);

			final byte[] insert_key = s2a(listName + (start - 1));
			final byte[] value = s2a(data[0].toString());

			kv.set(insert_key, value);

			kv.set(s2a(listName + "_s"), s2a((size + 1) + "")); // Increase size
			kv.set(s2a(listName + "_i"), s2a((start - 1) + "")); // Decrease
																	// start
		} else {
			for (Object d : data) {
				prepend(listName, d);
			}
		}
	}

	public void append(String listName, Object... data) {
		if (data == null || data.length == 0)
			return;
		if (data.length == 1) {
			final long size = getListSize(listName);
			final long start = getListStartIndex(listName);

			final byte[] insert_key = s2a(listName + (start + size));
			final byte[] value = s2a(data[0].toString());

			kv.set(insert_key, value);
			kv.set(s2a(listName + "_s"), s2a((size + 1) + ""));
			return;
		} else {
			for (Object d : data) {
				append(listName, d);
			}

		}
	}

	public String getFromList(String listName, long index) {
		return a2s(kv.get(s2a(listName + index)));
	}

	public void removeFromList(String listName, long index) {
		kv.remove(s2a(listName + index));
	}

	public void insertIntoList(String listName, long index, Object value) {
		kv.set(s2a(listName + index), s2a(value.toString()));
	}

	public long getListSize(String listName) {
		return longOrElse(kv.get(s2a(listName + "_s")), 0);
	}

	public long getListStartIndex(String listName) {
		return longOrElse(kv.get(s2a(listName + "_i")), 0);
	}

	public Object save(String bucket, Object key, Object data) {

		if (key == null) {
			key = UUID.randomUUID().toString();
		}

		if (data instanceof Map) {
			Map map = (Map) data;

			ArrayList<String> array = new ArrayList<String>();
			for (Object k : map.keySet()) {
				array.add(k.toString());
			}

			updateBucketFields(bucket, array.toArray(new String[array.size()]));
		}

		delete(bucket, key);

		byte[] realKey = s2a(bucket + key);
		byte json[] = encode(data);

		kv.set(realKey, json);
		updateIndex(bucket, key, data);

		kv.set(s2a(bucket + key), json);
		return key;
	}

	public void delete(String bucket, Object key) {
		removeFromIndex(bucket, key);
		kv.remove(s2a(bucket + key));
	}

	private void removeFromIndex(String bucket, Object key) {
		final Map data;
		try {
			data = get(bucket, Map.class, key);
		} catch (Exception ex) {
			return;
		}

		if (data == null)
			return;

		if (!schema.containsKey("buckets")) {
			return;
		}

		final Map<String, Map<String, Map<String, Object>>> buckets = schema.get("buckets");
		if (!buckets.containsKey(bucket)) {
			return;
		}
		final Map<String, Map<String, Object>> bucketMap = buckets.get(bucket);

		final Set<String> attributes = new HashSet<String>();
		for (Object k : data.keySet())
			attributes.add(k.toString());

		for (String attribute : attributes) {
			if (bucketMap.containsKey(attribute)) {
				String term = bucket + attribute + data.get(attribute);
				String docid = key.toString();

				unIndex(term, docid);

			}
		}
	}

	private void updateIndex(String bucket, Object key, Object data) {
		if (!(data instanceof Map))
			return;

		@SuppressWarnings("rawtypes")
		Map map = (Map) data;

		if (!schema.containsKey("buckets")) {
			return;
		}

		final Map<String, Map<String, Map<String, Object>>> buckets = schema.get("buckets");
		if (!buckets.containsKey(bucket)) {
			return;
		}

		final Map<String, Map<String, Object>> bucketMap = buckets.get(bucket);

		final Set<String> attributes = new HashSet<String>();
		for (Object k : map.keySet())
			attributes.add(k.toString());

		for (String attribute : attributes) {
			if (bucketMap.containsKey(attribute)) {
				boolean isIndexable = new Boolean(bucketMap.get(attribute).get("index").toString());
				if (isIndexable) {
					String term = bucket + attribute + map.get(attribute);
					String docid = key.toString();

					index(term, docid);
				}
			}
		}
	}

	public void index(String term, Object... value) {
		if (value == null || value.length == 0)
			return;

		if (value.length == 1) {
			String docid = value[0].toString();

			// Return if exists.
			if (kv.get(s2a(term + docid)) != null)
				return;

			final long size = longOrElse(kv.get(s2a(term + "_")), 0);
			if (size == 0) {
				kv.set(s2a(term + "_"), s2a("1"));
				kv.set(s2a(term + "0"), s2a(docid));
				kv.set(s2a(term + docid), s2a("0")); // reverse index
			} else {

				long incr_size = size + 1;
				kv.set(s2a(term + "_"), s2a(incr_size + ""));
				kv.set(s2a(term + size), s2a(docid));
				kv.set(s2a(term + docid), s2a(size + "")); // reverse index
			}
		} else {
			for (Object v : value) {
				index(term, v);
			}
		}
	}

	public void unIndex(String term, Object... value) {
		if (value == null || value.length == 0)
			return;

		if (value.length == 1) {
			String docid = value[0].toString();
			long value_index = longOrElse(kv.get(s2a(term + docid)), -1);
			if (value_index == -1)
				return; // Does Not Exist

			long size = longOrElse(kv.get(s2a(term + "_")), 0);

			if (size == 1) {
				// REMOVE ENTIRE THING
				kv.remove(s2a(term + "_"));
				kv.remove(s2a(term + value_index));
				kv.remove(s2a(term + docid));
				return;
			}

			kv.set(s2a(term + "_"), s2a((size - 1) + ""));

			if (value_index == (size - 1)) {
				// LAST ELEMENT, ONLY REMOVE NO SWAP
				kv.remove(s2a(term + value_index));
				kv.remove(s2a(term + docid)); // reverse index
			} else {

				// ANY MIDDLE ELEMENT
				// Create a copy of last element and its index.
				// Remove last element (element value at its index, element
				// index at its value)
				// insert last element

				byte[] last_element_value = kv.get(s2a(term + (size - 1)));

				// DELETE LAST ELEMENT's VALUE

				kv.remove(s2a(term + (size - 1))); // DELETE LAST VALUE index
				kv.remove(s2a(term + a2s(last_element_value))); // Last value
				kv.remove(s2a(term + docid)); // Last value

				kv.set(s2a(term + a2s(last_element_value)), s2a(value_index + ""));
				kv.set(s2a(term + value_index), last_element_value);

			}
		} else {
			for (Object v : value) {
				unIndex(term, v);
			}
		}
	}

	public <U> U get(String bucket, Class<U> valueType, Object... criteria) {
		if (criteria == null) {
			return null;
		}

		if (criteria.length == 1) {
			final byte[] key = s2a(bucket + criteria[0].toString());
			final byte[] json = kv.get(key);

			if (json == null)
				return null;
			return decode(json, valueType);
		}

		if (criteria.length % 2 != 0) {
			throw new RuntimeException("Expected Name Value pairs, got " + criteria.length);
		}

		Collection<U> collection = list(bucket, valueType, criteria);
		if (collection.iterator().hasNext()) {
			return collection.iterator().next();
		}
		return null;
	}

	public Collection<String> listIndexValues(final String... keys) {
		if (keys == null || keys.length == 0)
			return null;

		class IndexIterator extends AbstractCollection<String> implements Iterator<String> {

			String		term;
			long		index	= 0;
			final int	size;

			public IndexIterator(String term) {
				this.term = term;
				size = (int) longOrElse(kv.get(s2a(term + "_")), 0);
			}

			public Iterator<String> iterator() {
				return this;
			}

			public int size() {
				return size;
			}

			public boolean hasNext() {
				return index < size;
			}

			public String next() {
				index = index + 1;
				return a2s(kv.get(s2a(term + (index - 1))));
			}

			public void remove() {
				throw new RuntimeException("Not implemented");
			}
		}

		if (keys.length == 1) {
			return new IndexIterator(keys[0]);
		}

		class IndexIntersection extends AbstractCollection<String> implements Iterator<String> {

			final Iterator<String>	headIterator;
			final String[]			terms;
			String					next	= null;
			final int				size;

			public IndexIntersection(String... keys) {
				this.terms = keys;
				Collection<String> head = null;
				for (String key : keys) {
					Collection<String> next = listIndexValues(key);
					if (head == null)
						head = next;

					if (next.size() < head.size()) {
						head = next;
					}
				}
				headIterator = head.iterator();
				this.size = head.size();
			}

			public boolean hasNext() {
				while (headIterator.hasNext()) {
					final String docid = headIterator.next();
					boolean result = true;

					for (String term : terms) {
						if (result) {
							byte[] value = kv.get(s2a(term + docid));
							if (value == null) {
								result = false;
							}
						}
					}

					if (result) {
						this.next = docid;
						return true;
					}
				}

				return false;
			}

			public String next() {
				return next;
			}

			public void remove() {
				throw new RuntimeException("Not Implemented");
			}

			public Iterator<String> iterator() {
				return this;
			}

			/**
			 * Returns max size possible in intersection.
			 */
			public int size() {
				return size;
			}
		}

		return new IndexIntersection(keys);
	}

	private long longOrElse(Object obj, long els) {
		if (obj == null)
			return els;
		try {
			if (obj instanceof byte[]) {
				return Long.parseLong(a2s((byte[]) obj));
			} else {
				return Long.parseLong(obj.toString());
			}
		} catch (Exception ex) {
			return els;
		}
	}

	public <U> Collection<U> list(final String bucket, final Class<U> valueType, final Object... criteria) {

		class DocCollection extends AbstractCollection<U> implements Iterator<U> {

			final Iterator<String>	indexIterator;
			final int				size;

			public DocCollection() {
				String keys[] = new String[criteria.length / 2];

				for (int i = 0; i < criteria.length; i = i + 2) {
					keys[i / 2] = bucket + criteria[i] + criteria[i + 1].toString();
				}

				Collection<String> collection = listIndexValues(keys);

				indexIterator = collection.iterator();
				this.size = collection.size();
			}

			public boolean hasNext() {
				return indexIterator.hasNext();
			}

			public U next() {
				return get(bucket, valueType, indexIterator.next());
			}

			public void remove() {
				throw new RuntimeException("Not implemented");
			}

			public Iterator<U> iterator() {
				return this;
			}

			public int size() {
				return size;
			}

		}

		return new DocCollection();
	}

	public void updateBucketFields(String bucket, String... fields) {
		boolean changed = false;

		if (!schema.containsKey("buckets")) {
			changed = true;
			schema.put("buckets", new HashMap<String, Map<String, Map<String, Object>>>());
		}

		final Map<String, Map<String, Map<String, Object>>> buckets = schema.get("buckets");
		if (!buckets.containsKey(bucket)) {
			changed = true;
			buckets.put(bucket, new HashMap<String, Map<String, Object>>());
		}
		final Map<String, Map<String, Object>> bucketMap = buckets.get(bucket);

		for (String key : fields) {
			if (!bucketMap.containsKey(key)) {
				changed = true;
				Map<String, Object> keyMap = new HashMap<String, Object>();
				keyMap.put("index", false); // Default is false
				bucketMap.put(key, keyMap);
			}
		}

		if (changed) {
			final byte[] json = encode(schema);
			final byte[] key = s2a("schema");
			kv.set(key, json);
		}
	}

	public void ensureIndex(String bucket, String... fields) {
		boolean changed = false;

		if (!schema.containsKey("buckets")) {
			changed = true;
			schema.put("buckets", new HashMap<String, Map<String, Map<String, Object>>>());
		}

		final Map<String, Map<String, Map<String, Object>>> buckets = schema.get("buckets");

		if (!buckets.containsKey(bucket)) {
			changed = true;
			buckets.put(bucket, new HashMap<String, Map<String, Object>>());
		}

		final Map<String, Map<String, Object>> bucketMap = buckets.get(bucket);

		for (String key : fields) {
			if (!bucketMap.containsKey(key)) {
				Map<String, Object> keyMap = new HashMap<String, Object>();
				keyMap.put("index", true);
				bucketMap.put(key, keyMap);
			} else {
				Map<String, Object> keyMap = bucketMap.get(key);
				final boolean isIndexed = new Boolean(keyMap.get("index").toString());
				if (!isIndexed) {
					changed = true;
					keyMap.put("indexed", true);
				}
			}
		}

		if (changed) {
			final byte[] json = encode(schema);
			final byte[] key = s2a("schema");
			kv.set(key, json);
		}
	}

	public void unEnsureIndex(String bucket, String... fields) {
		if (!schema.containsKey("buckets"))
			return;
		if (!schema.get("buckets").containsKey(bucket))
			return;

		Map<String, Map<String, Object>> bucketMap = schema.get("buckets").get(bucket);
		boolean changed = false;

		for (String key : fields) {
			if (!bucketMap.containsKey(key)) {
				return;
			} else {

				boolean isIndexed = new Boolean(bucketMap.get(key).get("index").toString());
				if (isIndexed) {
					changed = true;
					bucketMap.get(key).put("index", false);
				}

			}
		}

		if (changed) {
			byte[] schema_json = encode(schema);
			kv.set(s2a("schema"), schema_json);
		}
	}

	private String a2s(byte[] a) {
		/** Converts Array to String **/
		if (a == null)
			return null;
		try {
			return new String(a, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			return new String(a);
		}
	}

	private byte[] s2a(String s) {
		if (s == null)
			return null;
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException ex) {
			return s.getBytes();
		}
	}

	final static ObjectMapper	mapper	= new ObjectMapper();

	public static <T> T decode(byte[] array, Class<T> valueType) {
		if (array == null)
			return null;
		try {
			return mapper.readValue(array, valueType);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static byte[] encode(Object object) {
		try {
			return mapper.writeValueAsBytes(object);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		kv.close();
	}

}
