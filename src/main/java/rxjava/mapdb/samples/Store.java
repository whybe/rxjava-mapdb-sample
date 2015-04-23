package rxjava.mapdb.samples;

public interface Store {
	public StoreSubscription makeStoreSubscription();
	public String getMapName();
}
