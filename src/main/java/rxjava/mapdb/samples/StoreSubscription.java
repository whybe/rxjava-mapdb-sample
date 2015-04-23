package rxjava.mapdb.samples;

import org.mapdb.DB;
import org.mapdb.TxMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Subscription;

public class StoreSubscription implements Subscription {
	private final static Logger logger = LoggerFactory.getLogger(StoreSubscription.class);

	private final TxMaker tx;
	private final DB db;

	public StoreSubscription(DB db) {
		this.db = db;
		this.tx = null;
	}

	public StoreSubscription(TxMaker tx) {
		this.db = null;
		this.tx = tx;
		logger.info("Using Transaction");
	}

	public DB getDB() {
		return db;
	}

	public TxMaker getTx() {
		return tx;
	}

	private boolean isUsingTransaction() {
		return (tx != null) ? true : false;
	}

	@Override public void unsubscribe() {
		if (isUsingTransaction()) {
			tx.execute((db) -> {
				db.close();
				logger.info("close : {}", db);
			});
		} else {
			db.close();
			logger.info("close : {}", db);
		}
		
	}

	@Override public boolean isUnsubscribed() {
		if (isUsingTransaction()) {
			return tx.execute((db) -> {
				return db.isClosed();
			});
		} else {
			return db.isClosed();
		}
	}
}
