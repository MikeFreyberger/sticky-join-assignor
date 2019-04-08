import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by mfreyberger on 5/31/18.
 */
public class StickyJoinPartitionAssignor implements PartitionAssignor {
    private static final Logger logger = LoggerFactory.getLogger(StickyJoinPartitionAssignor.class);

    // these schemas are used for preserving consumer's previously assigned partitions
    // list and sending it as user data to the leader during a rebalance
    private static final String PARTITIONS_KEY_NAME = "partitions";
    private static final String EPOCH_KEY_NAME = "epoch";
    private static final Schema PARTITION_ASSIGNMENT = new Schema(
            new Field(PARTITIONS_KEY_NAME, new ArrayOf(Type.INT32)),
            new Field(EPOCH_KEY_NAME, Type.INT32));

    private static final Schema EPOCH_ASSIGNMENT = new Schema(
            new Field(EPOCH_KEY_NAME, Type.INT32));

    private Set<Integer> memberPartitionAssignment = null;
    private Integer epoch = 0;

    public String name() {
        return "sticky-join";
    }

    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());

        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                logger.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);
        Integer nextEpoch = getMaxEpoch(subscriptions) + 1;
        logger.info("Setting epoch to {} for all consumers in this assignment", nextEpoch);
        ByteBuffer epochSerialized = serializeEpoch(nextEpoch);

        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue(), epochSerialized));
        return assignments;
    }

    private static Integer getMaxEpoch(Map<String, Subscription> subscriptions) {
        logger.info("Determining current max epoch.");
        Integer maxEpoch = 0;
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            ByteBuffer userData = subscriptionEntry.getValue().userData();
            if (userData != null && userData.hasRemaining()) {
                PartitionAssignment prevAssignment = deserializePartitionAssignment(userData);
                Integer epoch = prevAssignment.epoch;
                if (epoch > maxEpoch) {
                    maxEpoch = epoch;
                }
            }
        }
        logger.info("Current max epoch: {}", maxEpoch);
        return maxEpoch;
    }

    private Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic, Map<String, Subscription> subscriptions) {
        Map<String, List<TopicPartition>> finalAssignment = new HashMap<>();
        int partitionsEligible = determinePartitionsEligible(partitionsPerTopic);
        Map<String, List<Integer>> previousAssignment = getPreviousAssignment(subscriptions, partitionsEligible);
        Map<String, List<Integer>> newAssignment = new HashMap<>();
        final Set<String> consumers = subscriptions.keySet();

        logger.info("Previous Assignment");
        printAssignment(previousAssignment);

        List<Integer> currentlyUnownedPartitions = determineUnownedPartitions(partitionsEligible, previousAssignment);
        int draftRound = 0;
        int partitionsDrafted = 0;
        List<String> consumersToStealSorted = determineConsumersToStealList(previousAssignment);

        int consumerToStealFromIdx = 0;
        int numConsumers = subscriptions.size();
        List<String> draftOrder = getDraftOrder(previousAssignment, consumers);
        logger.info("Initial draft order:");
        for (String consumer : draftOrder) {
            logger.info("\t{}", consumer);
        }
        int draftIdx = 0;

        while (partitionsDrafted < partitionsEligible) {
            logger.info("Draft Round: {}", draftRound);
            logger.info("Draft Index: {}", draftIdx);
            String consumerDrafting = draftOrder.get(draftIdx);
            logger.info("{} is drafting", consumerDrafting);

            if (previousAssignment.containsKey(consumerDrafting) && previousAssignment.get(consumerDrafting).size() > 0) {
                List<Integer> partitionsPreviouslyOwned = previousAssignment.get(consumerDrafting);
                Integer partitionDrafted = partitionsPreviouslyOwned.remove(0);
                logger.info("{} is drafting {} from a previous assignment", consumerDrafting, partitionDrafted);
                put(newAssignment, consumerDrafting, partitionDrafted);
            } else if (currentlyUnownedPartitions.size() > 0) {
                Integer partitionDrafted = currentlyUnownedPartitions.remove(0);
                logger.info("{} is drafting {} from the unowned list", consumerDrafting, partitionDrafted);
                put(newAssignment, consumerDrafting, partitionDrafted);
            } else {
                String consumerToSteal = consumersToStealSorted.get(consumerToStealFromIdx);
                logger.info("{} is stealing from {}", consumerDrafting, consumerToSteal);
                List<Integer> partitionsPreviouslyOwned = previousAssignment.get(consumerToSteal);
                Integer partitionDrafted = partitionsPreviouslyOwned.remove(0);
                logger.info("{} is drafting {} from a previous assignment that was owned by {}", consumerDrafting, partitionDrafted, consumerToSteal);
                put(newAssignment, consumerDrafting, partitionDrafted);
                consumerToStealFromIdx = ++consumerToStealFromIdx % consumersToStealSorted.size();
                logger.info("{} is the new steal idx", consumerToStealFromIdx);
            }

            partitionsDrafted++;
            draftIdx++;
            if (draftIdx == numConsumers) {
                logger.info("End of draft round {}", draftRound);
                draftIdx = 0;
                draftOrder = getDraftOrder(previousAssignment, consumers);
                logger.info("Creating new draft order:");
                for (String consumer : draftOrder) {
                    logger.info("\t{}", consumer);
                }
                draftRound++;
            }
        }

        logger.info("New Assignment");
        printAssignment(newAssignment);

        for (Map.Entry<String, List<Integer>> entry : newAssignment.entrySet()) {
            String consumer = entry.getKey();
            List<String> topics = subscriptions.get(consumer).topics();
            List<Integer> partitions = entry.getValue();
            for (Integer partition: partitions) {
                for (String topic : topics) {
                    put(finalAssignment, consumer, new TopicPartition(topic, partition));
                }
            }
        }

        return finalAssignment;
    }

    private static void printAssignment(Map<String, List<Integer>> partitionAssignment) {
        if (partitionAssignment.size() == 0) {
            logger.info("\tEmpty");
        }
        for (Map.Entry<String, List<Integer>> entry : partitionAssignment.entrySet()) {
            logger.info("\tResolverKafkaConsumer {}", entry.getKey());
            for (Integer partition : entry.getValue()) {
                logger.info("\t\t{}", partition);
            }
        }
    }

    private static List<String> getDraftOrder(Map<String, List<Integer>> previousAssignment, Set<String> consumers) {
        List<String> draftOrder = new ArrayList<>();
        Set<String> consumersRemaining = new HashSet<>(consumers);

        for (String consumer : consumers) {
            if (previousAssignment.containsKey(consumer) && previousAssignment.get(consumer).size() > 0) {
                draftOrder.add(consumer);
                consumersRemaining.remove(consumer);
            }
        }

        draftOrder.addAll(consumersRemaining);

        return draftOrder;
    }

    private static List<String> determineConsumersToStealList(Map<String, List<Integer>> previousAssignment) {
        logger.info("Determining the list of consumers to steal from based on the previous assignment");
        List<Map.Entry<String, List<Integer>>> consumersToSteal = new ArrayList<>(previousAssignment.entrySet());
        consumersToSteal.sort(new Comparator<Map.Entry<String, List<Integer>>>() {
            @Override
            public int compare(Map.Entry<String,List<Integer>> e1, Map.Entry<String,List<Integer>> e2) {
                Integer e1Size = e1.getValue().size();
                Integer e2Size = e2.getValue().size();
                return e2Size.compareTo(e1Size);
            }
        });
        List<String> result = new ArrayList<>();
        logger.info("Final list of consumers to steal from:");
        for (Map.Entry<String, List<Integer>> entry : consumersToSteal) {
            logger.info("\t{} who had {} partitions", entry.getKey(), entry.getValue().size());
            result.add(entry.getKey());
        }
        return result;
    }

    private static List<Integer> determineUnownedPartitions(int eligiblePartitions, Map<String, List<Integer>> previousAssignment) {
        Set<Integer> unownedPartitions = new HashSet<>();
        for (int i = 0; i < eligiblePartitions; i++) {
            unownedPartitions.add(i);
        }
        for (Map.Entry<String, List<Integer>> entry : previousAssignment.entrySet()) {
            for (Integer partition : entry.getValue()) {
                unownedPartitions.remove(partition);
            }
        }

        List<Integer> unownedList = new ArrayList<>();
        unownedList.addAll(unownedPartitions);
        logger.info("Unowned Partitions:");
        for (Integer partition : unownedPartitions) {
            logger.info("\t{}", partition);
        }

        return unownedList;
    }

    public void onAssignment(Assignment assignment) {
        logger.info("Received assignment.");
        epoch = deserializeEpoch(assignment.userData());
        logger.info("Epoch: {}", epoch);
        memberPartitionAssignment = new HashSet<>();
        for (TopicPartition topPar : assignment.partitions()) {
            memberPartitionAssignment.add(topPar.partition());
        }
        logger.info("Partitions:");
        for (Integer partition : memberPartitionAssignment) {
            logger.info("\t{}", partition);
        }
    }

    public Subscription subscription(Set<String> topics) {
        logger.info("Creating subscription");
        if (memberPartitionAssignment == null) {
            logger.info("No previous member partition assignment. Creating subscription with just topic list");
            return new Subscription(new ArrayList<>(topics));
        }

        logger.info("Previous member partition assignment found. Passing previous assignment as user data");
        logger.info("Previous partition assignment:");
        for (Integer partition : memberPartitionAssignment) {
            logger.info("\t{}", partition);
        }
        logger.info("Passing epoch:");
        logger.info("\t{}", epoch);
        return new Subscription(new ArrayList<>(topics), serializePartitionAssignment(memberPartitionAssignment, epoch));
    }

    private static int determinePartitionsEligible(Map<String, Integer> partitionsPerTopic) {
        int partitionsInConsideration = 0;
        boolean isSet = false;
        for (Map.Entry<String, Integer> topicEntry : partitionsPerTopic.entrySet()) {
            Integer partitionCount = topicEntry.getValue();
            if (!isSet) {
                logger.info("{} has {} partitions and has initialized the partition count", topicEntry.getKey(), partitionCount);
                partitionsInConsideration = partitionCount;
                isSet = true;
            }
            if (partitionCount < partitionsInConsideration) {
                logger.info("{} has {} partitions and has adjusted the partition count", topicEntry.getKey(), partitionCount);
                partitionsInConsideration = partitionCount;
            }
        }
        logger.info("{} partitions are eligible for assignment", partitionsInConsideration);
        return partitionsInConsideration;
    }

    private static Map<String, List<Integer>> getPreviousAssignment(Map<String, Subscription> subscriptions, int partitionsEligible) {
        logger.info("Determining previous assignment");
        Map<Integer, PartitionOwner> partitions2Owners = new HashMap<>();

        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet()) {
            String consumer = subscriptionEntry.getKey();
            logger.info("\tChecking consumer {}", consumer);
            ByteBuffer userData = subscriptionEntry.getValue().userData();
            if (userData != null && userData.hasRemaining()) {
                PartitionAssignment prevAssignment = deserializePartitionAssignment(userData);
                Integer epoch = prevAssignment.epoch;
                for (Integer partition : prevAssignment.partitions) {
                    if (partition > partitionsEligible) {
                        logger.info("Skipping consumer {}'s ownership of partition {} because that partition is no longer eligible. Only partitions [0,{}) are eligible.",
                                consumer, partition, partitionsEligible);
                    }
                    if(partitions2Owners.containsKey(partition)) {
                        PartitionOwner exisitingOwner = partitions2Owners.get(partition);
                        if (exisitingOwner.epoch < epoch) {
                            logger.info("{} claimed ownership of {} with epoch {}, but {} ownership has epoch {} which is greater",
                                    exisitingOwner.consumer, partition, exisitingOwner.epoch, consumer, epoch);
                            partitions2Owners.put(partition, new PartitionOwner(consumer, epoch));
                        } else {
                            logger.info("{} claimed ownership of {} with epoch {}. {} tried to take it, but only has epoch {}, which is less",
                                    exisitingOwner.consumer, partition, exisitingOwner.epoch, consumer, epoch);
                        }

                    } else {
                        logger.info("Setting {} as the previous owner of {} with epoch {}", consumer, partition, epoch);
                        partitions2Owners.put(partition, new PartitionOwner(consumer, epoch));
                    }
                }
            } else {
                logger.info("\t\tUser data is empty");
            }
        }
        Map<String, List<Integer>> previousAssignment = new HashMap<>();
        for (Map.Entry<Integer, PartitionOwner> entry : partitions2Owners.entrySet()) {
            put(previousAssignment, entry.getValue().consumer, entry.getKey());
        }

        return previousAssignment;
    }

    private static ByteBuffer serializePartitionAssignment(Set<Integer> partitions, Integer epoch) {
        logger.info("Serializing partition assignment");
        Struct struct = new Struct(PARTITION_ASSIGNMENT);

        struct.set(PARTITIONS_KEY_NAME, partitions.toArray());
        struct.set(EPOCH_KEY_NAME, epoch);
        ByteBuffer buffer = ByteBuffer.allocate(PARTITION_ASSIGNMENT.sizeOf(struct));
        PARTITION_ASSIGNMENT.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    private static PartitionAssignment deserializePartitionAssignment(ByteBuffer buffer) {
        try {
            logger.info("\t\tAttempting to deserialize user data");
            logger.info("\t\tPosition: {}, Limit: {}, Str: ", buffer.position(), buffer.limit(), buffer.toString());
            Struct struct = PARTITION_ASSIGNMENT.read(buffer);
            Integer epoch = struct.getInt(EPOCH_KEY_NAME);
            List<Integer> partitions = new ArrayList<>();
            for (Object structObj : struct.getArray(PARTITIONS_KEY_NAME)) {
                Integer partition = (Integer) structObj;
                logger.info("\t\t\tFound partition {}", partition);
                partitions.add(partition);
            }
            buffer.position(0);
            return new PartitionAssignment(partitions, epoch);
        } catch (SchemaException e) {
            logger.error(e.getMessage());
            return new PartitionAssignment(new ArrayList<>(), 0);
        }
    }

    private static ByteBuffer serializeEpoch(Integer epoch) {
        logger.info("Serializing epoch");
        Struct struct = new Struct(EPOCH_ASSIGNMENT);

        struct.set(EPOCH_KEY_NAME, epoch);
        ByteBuffer buffer = ByteBuffer.allocate(EPOCH_ASSIGNMENT.sizeOf(struct));
        EPOCH_ASSIGNMENT.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    private static Integer deserializeEpoch(ByteBuffer buffer) {
        try {
            Struct struct = EPOCH_ASSIGNMENT.read(buffer);
            buffer.position(0);
            return struct.getInt(EPOCH_KEY_NAME);
        } catch (SchemaException e) {
            logger.error(e.getMessage());
            return 0;
        }
    }

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.add(value);
    }

    static class PartitionAssignment {
        List<Integer> partitions;
        Integer epoch;

        PartitionAssignment(List<Integer> partitions, Integer epoch) {
            this.partitions = partitions;
            this.epoch = epoch;
        }
    }

    static class PartitionOwner {
        String consumer;
        Integer epoch;

        PartitionOwner(String consumer, Integer epoch) {
            this.consumer = consumer;
            this.epoch = epoch;
        }
    }
}
