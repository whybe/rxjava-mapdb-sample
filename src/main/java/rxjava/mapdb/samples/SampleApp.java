package rxjava.mapdb.samples;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observer;

public class SampleApp {
	private final static Logger logger = LoggerFactory.getLogger(SampleApp.class);

	public static final String MULTIMAP_NAME_STRING_AND_BYTE_ARRAY = "multimapsba";
	public static final String RESOURCE_PATH = "src/main/resources/rxjava/mapdb/samples";
	public static final String DB_FILE = "sample.mapdb";
	private static final String MAP_NAME = "sample";

	public static void main(String[] args) throws IOException {
		File dbFile = new File(RESOURCE_PATH, DB_FILE);
		if (!dbFile.exists()) {
			FileUtils.touch(dbFile);
		}
		SampleDataService service = new SampleDataService(new SampleDataStore(new File(
				RESOURCE_PATH, DB_FILE), MAP_NAME));
		service.storeDataObservable(new SampleData("1", "a"))
				.subscribe(new Observer<Boolean>() {

					@Override public void onCompleted() {
						logger.info("onCompleted");
					}

					@Override public void onError(Throwable e) {
						logger.error(e.getMessage(), e);
					}

					@Override public void onNext(Boolean b) {
						logger.info("onNext : {}", b);
					}
				});
		
		service.fetchDataObservable()
				.subscribe(new Observer<SampleData>() {

					@Override public void onCompleted() {
						logger.info("onCompleted");
					}

					@Override public void onError(Throwable e) {
						logger.error(e.getMessage(), e);
					}

					@Override public void onNext(SampleData data) {
						logger.info("({}, {})", data.getKey(), data.getValue());
					}
				});

	}
}
