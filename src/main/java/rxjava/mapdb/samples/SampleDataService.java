package rxjava.mapdb.samples;

import java.rmi.UnexpectedException;
import java.util.ArrayList;
import java.util.List;

import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.TxRollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func1;

public class SampleDataService {
	private final static Logger logger = LoggerFactory.getLogger(StoreSubscription.class);

	private final Store store;

	public SampleDataService(Store store) {
		this.store = store;
	}

//	public Observable<Void> storeDataObservable(SampleData data) {
//		return Observable.using(store::makeStoreSubscription,
//				new Func1<StoreSubscription, Observable<Void>>() {
//
//					@Override public Observable<Void> call(StoreSubscription subscription) {
//
//						BTreeMap<String, String> treeMap = subscription.getDB().createTreeMap(store
//								.getMapName()).makeOrGet();
//
//						return Observable.create(new OnSubscribe<Void>() {
//
//							@Override public void call(
//									Subscriber<? super Void> subscriber) {
//								treeMap.put(data.getKey(), data.getValue());
//								logger.info("put : ({}, {})", data.getKey(), data.getValue());
//								subscription.getDB().commit();
//								subscriber.onCompleted();
//							}
//						});
//					}
//				}, new Action1<StoreSubscription>() {
//
//					@Override public void call(StoreSubscription subscription) {
////						subscription.unsubscribe();
//					}
//				});
//	}
//
//	public Observable<SampleData> fetchDataObservable() {
//		return Observable.using(store::makeStoreSubscription,
//				new Func1<StoreSubscription, Observable<SampleData>>() {
//
//					@Override public Observable<SampleData> call(StoreSubscription subscription) {
//
//						BTreeMap<String, String> treeMap = subscription.getDB().createTreeMap(store
//								.getMapName()).makeOrGet();
//
//						return Observable.create(new OnSubscribe<SampleData>() {
//
//							@Override public void call(
//									Subscriber<? super SampleData> subscriber) {
//								for (String key : treeMap.keySet()) {
//									logger.info("get : ({}, {})", key, treeMap.get(key));
//									subscriber.onNext(new SampleData(key, treeMap
//											.get(key)));
//								}
//								subscription.getDB().commit();
//								subscriber.onCompleted();
//
//							}
//						});
//					}
//				}, new Action1<StoreSubscription>() {
//
//					@Override public void call(StoreSubscription subscription) {
////						subscription.unsubscribe();
//					}
//				});
//	}

	public Observable<Boolean> storeDataObservable(SampleData data) {
		return Observable.using(
				store::makeStoreSubscription,
//				(subscription) -> {
//					return Observable.create(
//							(subscriber) -> {
//								subscriber.onNext(subscription.getTx().execute(
//										(db) -> {
//											BTreeMap<String, String> treeMap = db.createTreeMap(
//													store.getMapName()).makeOrGet();
//											treeMap.put(data.getKey(), data.getValue());
//											logger.info("put : ({}, {})", data.getKey(), data.getValue());
//											return Boolean.TRUE;
//										}));
//
//								subscriber.onCompleted();
//							});
//				},
//				(subscription) -> {
//					subscription.unsubscribe();
//				});
				new Func1<StoreSubscription, Observable<Boolean>>() {

					@Override public Observable<Boolean> call(StoreSubscription subscription) {

						return Observable.create(new OnSubscribe<Boolean>() {

							@Override public void call(Subscriber<? super Boolean> subscriber) {
								subscriber.onNext(subscription.getTx()
										.execute(new Fun.Function1<Boolean, DB>() {

											@Override public Boolean run(DB db) {
												BTreeMap<String, String> treeMap = db
														.createTreeMap(store
																.getMapName()).makeOrGet();
												treeMap.put(data.getKey(), data.getValue());
												logger.info("put : ({}, {})", data.getKey(),
														data.getValue());
//												throw new UnexpectedException("Exception Test");
												
												return Boolean.TRUE;
											}
										}));

								subscriber.onCompleted();
							}
						});
					}
				}, new Action1<StoreSubscription>() {

					@Override public void call(StoreSubscription subscription) {
						subscription.unsubscribe();
					}
				});
	}

	public Observable<SampleData> fetchDataObservable() {
		return Observable.using(store::makeStoreSubscription,
				new Func1<StoreSubscription, Observable<SampleData>>() {

					@Override public Observable<SampleData> call(StoreSubscription subscription) {

						return Observable.from(subscription.getTx()
								.execute(new Fun.Function1<List<SampleData>, DB>() {

									@Override public List<SampleData> run(DB db) {
										BTreeMap<String, String> treeMap = db.createTreeMap(store
												.getMapName()).makeOrGet();

										List<SampleData> list = new ArrayList<SampleData>();

										for (String key : treeMap.keySet()) {
											String value = treeMap.get(key);
											list.add(new SampleData(key, value));
											logger.info("get : ({}, {})", key, value);
										}

										return list;
									}
								}));

					}
				}, new Action1<StoreSubscription>() {

					@Override public void call(StoreSubscription subscription) {
						subscription.unsubscribe();
					}
				});
	}

	public Store getStore() {
		return store;
	}
}
