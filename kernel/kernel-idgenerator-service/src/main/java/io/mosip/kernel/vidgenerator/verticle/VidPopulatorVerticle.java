package io.mosip.kernel.vidgenerator.verticle;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

import io.mosip.kernel.core.idgenerator.spi.VidGenerator;
import io.mosip.kernel.vidgenerator.constant.EventType;
import io.mosip.kernel.vidgenerator.constant.VidLifecycleStatus;
import io.mosip.kernel.vidgenerator.entity.VidEntity;
import io.mosip.kernel.vidgenerator.generator.VidWriter;
import io.mosip.kernel.vidgenerator.utils.VIDMetaDataUtil;
import io.vertx.core.AbstractVerticle;
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

            int batchSize = 5000; // Optimized batch size
            int adjustedBatchSize = (int) Math.min(batchSize, noOfVidsToGenerate);
            Set<String> generatedSet = new HashSet<>((int) noOfVidsToGenerate);

            vertx.executeBlocking(promise -> {
            	long startTime = System.nanoTime(); // ⏱️ Start timing
                try {
                    long count = 0;
                    List<VidEntity> batch = new ArrayList<>(batchSize);

                    while (count < noOfVidsToGenerate) {
                        // Pre-generate candidates to reduce failed attempts
                        List<String> candidates = new ArrayList<>(10);
                        for (int i = 0; i < 10 && count < noOfVidsToGenerate; i++) {
                            candidates.add(vidGenerator.generateId());
                        }
                        for (String vid : candidates) {
                            if (generatedSet.add(vid)) {
                                VidEntity entity = new VidEntity();
                                entity.setVid(vid);
                                entity.setStatus(VidLifecycleStatus.AVAILABLE);
                                metaDataUtil.setCreateMetaData(entity);
                                batch.add(entity);
                                count++;
                                break;
                            }
                        }

                        if (batch.size() >= adjustedBatchSize  || count == noOfVidsToGenerate) {
                            int inserted = vidWriter.persistVidsInBulk(batch);
                            if (inserted < batch.size()) {
                                LOGGER.warn("Duplicates detected, inserted {} of {} VIDs, retrying individually...", inserted, batch.size());
                                // Retry failed VIDs individually
                                List<VidEntity> failedBatch = new ArrayList<>(batch.subList(inserted, batch.size()));
                                batch.clear();
                                for (VidEntity entityToRetry : failedBatch) {
                                    if (generatedSet.contains(entityToRetry.getVid())) {
                                        List<VidEntity> single = new ArrayList<>(1);
                                        single.add(entityToRetry);
                                        int singleInserted = vidWriter.persistVidsInBulk(single);
                                        if (singleInserted == 0) {
                                            LOGGER.warn("Failed VID: {}, will retry later", entityToRetry.getVid());
                                            count--;
                                            generatedSet.remove(entityToRetry.getVid());
                                        }
                                    } else {
                                        count--;
                                    }
                                }
                            } else {
                                batch.clear();
                            }
                        }
                    }
                    long endTime = System.nanoTime(); // ⏱️ End timing
                    long durationMillis = (endTime - startTime) / 1_000_000;

                    LOGGER.info("✅ Total VIDs persisted: {}", count);
                    LOGGER.info("⏱️ Total time taken for VIDs persisted: {} ms (~{} seconds)", durationMillis, durationMillis / 1000);
                    promise.complete(count);
                } catch (Exception e) {
                    LOGGER.error("❌ Error during VID pool generation", e);
                    promise.fail(e);
                }
            }, res -> {
                if (res.succeeded()) {
                    handler.reply("pool population successful");
                } else {
                    handler.fail(500, "VID generation failed");
                }
            });
        });

        startFuture.complete(); // Mark verticle startup as successful
    }
}