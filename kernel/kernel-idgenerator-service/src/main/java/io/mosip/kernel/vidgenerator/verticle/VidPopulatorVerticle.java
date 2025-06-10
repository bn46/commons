package io.mosip.kernel.vidgenerator.verticle;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import io.mosip.kernel.core.idgenerator.spi.VidGenerator;
import io.mosip.kernel.vidgenerator.constant.EventType;
import io.mosip.kernel.vidgenerator.constant.VidLifecycleStatus;
import io.mosip.kernel.vidgenerator.entity.VidEntity;
import io.mosip.kernel.vidgenerator.generator.VidWriter;
import io.mosip.kernel.vidgenerator.utils.VIDMetaDataUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class VidPopulatorVerticle extends AbstractVerticle {

	private static final Logger LOGGER = LoggerFactory.getLogger(VidPopulatorVerticle.class);

	private long vidToGenerate;

	private Environment environment;

	private VidWriter vidWriter;

	private VIDMetaDataUtil metaDataUtil;

	private VidGenerator<String> vidGenerator;

	@SuppressWarnings("unchecked")
	public VidPopulatorVerticle(final ApplicationContext context) {
		this.environment = context.getBean(Environment.class);
		this.vidToGenerate = Objects.requireNonNullElse(environment.getProperty("mosip.kernel.vid.vids-to-generate", Long.class), 0L);
		this.vidWriter = context.getBean("vidWriter", VidWriter.class);
		this.metaDataUtil = context.getBean(VIDMetaDataUtil.class);
		this.vidGenerator = context.getBean(VidGenerator.class);
	}

	/*
	 * @Override public void start(Future<Void> startFuture) throws Exception {
	 * vertx.eventBus().consumer(EventType.GENERATEPOOL, handler -> { long
	 * noOfFreeVids = Long.parseLong(handler.body().toString()); long
	 * noOfVidsToGenerate = vidToGenerate - noOfFreeVids;
	 * LOGGER.info("Persisting {} vids in pool", noOfVidsToGenerate); long count =
	 * 0; while (count < vidToGenerate) { String vid = vidGenerator.generateId();
	 * VidEntity entity = new VidEntity(); entity.setVid(vid);
	 * entity.setStatus(VidLifecycleStatus.AVAILABLE);
	 * metaDataUtil.setCreateMetaData(entity); boolean isPersisted =
	 * vidWriter.persistVids(entity); if (isPersisted) { count++; } }
	 * handler.reply("pool population successfull");
	 * 
	 * LOGGER.info("No of vids persisted are {}", count); }); }
	 */
	
	@Override
	public void start(io.vertx.core.Future<Void> startFuture) {
		vertx.eventBus().consumer(EventType.GENERATEPOOL, handler -> {
			long noOfFreeVids = Long.parseLong(handler.body().toString());
			long noOfVidsToGenerate = vidToGenerate - noOfFreeVids;
			LOGGER.info("Persisting {} vids in pool", noOfVidsToGenerate);

			int threads = 4;
			int batchSize = 500;
			long perThreadTarget = noOfVidsToGenerate / threads;
			long remaining = noOfVidsToGenerate % threads;

			Set<String> generatedSet = ConcurrentHashMap.newKeySet();
			ExecutorService executor = Executors.newFixedThreadPool(threads);
			List<Callable<Integer>> tasks = new ArrayList<>();

			for (int t = 0; t < threads; t++) {
				long finalTarget = perThreadTarget + (t == threads - 1 ? remaining : 0); // last thread gets the remainder
				tasks.add(() -> {
					int inserted = 0;
					List<VidEntity> batch = new ArrayList<>();
					while (inserted < finalTarget) {
						String vid = vidGenerator.generateId();
						if (generatedSet.add(vid)) {
							VidEntity entity = new VidEntity();
							entity.setVid(vid);
							entity.setStatus(VidLifecycleStatus.AVAILABLE);
							metaDataUtil.setCreateMetaData(entity);
							batch.add(entity);
						}
						if (batch.size() == batchSize) {
							inserted += vidWriter.persistVidsInBulk(batch);
							batch.clear();
						}
					}
					if (!batch.isEmpty()) {
						inserted += vidWriter.persistVidsInBulk(batch);
					}
					return inserted;
				});
			}

			vertx.executeBlocking(promise -> {
				try {
					int totalInserted = 0;
					List<java.util.concurrent.Future<Integer>> results = executor.invokeAll(tasks);
					for (java.util.concurrent.Future<Integer> result : results) {
						totalInserted += result.get();
					}
					LOGGER.info("✅ Total VIDs persisted: {}", totalInserted);
					promise.complete(totalInserted);
				} catch (Exception e) {
					LOGGER.error("❌ Error during VID pool generation", e);
					promise.fail(e);
				} finally {
					executor.shutdown();
				}
			}, res -> {
				if (res.succeeded()) {
					handler.reply("pool population successful");
				} else {
					handler.fail(500, "VID generation failed");
				}
			});
		});

		startFuture.complete(); // ✅ mark verticle startup as successful
	}
}