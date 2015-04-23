package rxjava.mapdb.samples;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;

public class SampleDataStore implements Store {
	private final File dbFile;
	private final String mapName;
	
	public SampleDataStore(File dbFile, String mapName) {
		this.dbFile = dbFile;
		this.mapName = mapName;
	}
	
	@Override public StoreSubscription makeStoreSubscription() {
		return new StoreSubscription(makeTransaction());
//		return new StoreSubscription(makeDB());
	}

	@Override public String getMapName() {
		return mapName;
	}

	private TxMaker makeTransaction() {

		return DBMaker.fileDB(dbFile)
//				.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
//				.mmapFileEnable() //occur error when db is closed.
//				.cacheDisable() // workaround internal error
//				.commitFileSyncDisable()
//				.asyncWriteFlushDelay(100)
//				.lockSingleEnable()
				.closeOnJvmShutdown()
//				.deleteFilesAfterClose()
				.makeTxMaker();
	}
	
	private DB makeDB() {

		return DBMaker.fileDB(dbFile)
//				.mmapFileEnableIfSupported() // need JVM(7+), it uses RAF by default.
//				.mmapFileEnable() //occur error when db is closed.
//				.cacheDisable() // workaround internal error
//				.commitFileSyncDisable()
//				.asyncWriteFlushDelay(100)
//				.lockSingleEnable()
				.closeOnJvmShutdown()
//				.deleteFilesAfterClose()
				.make();
	}
}
