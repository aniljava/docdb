package docdb;

/**
 * This interface is meant to be implemented by the underlying persistence provider. For any new backend like (memory, database, other kv engines), this is the only
 * method to implement.
 */
public interface KV {
	public byte[] get(byte[] key);
	public void set(byte[] key, byte[] value);
	public void remove(byte[] key);
	public Object getKV();
	public void close();
}
