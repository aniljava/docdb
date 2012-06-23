package docdb;

public interface KV {
	public byte[] get(byte[] key);
	public void set(byte[] key, byte[] value);
	public void remove(byte[] key);
	public Object getKV();
	public void close();
}
