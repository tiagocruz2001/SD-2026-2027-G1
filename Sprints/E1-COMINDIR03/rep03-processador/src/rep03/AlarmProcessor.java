package rep03;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.io.IOException;

public class AlarmProcessor {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }

        int count = Integer.parseInt(args[0]);
        String api = args.length > 1
                ? args[1]
                : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub pubSub = new IpfsPubSub(api);

        final Partitioner partitioner = new Partitioner(count);
        final long[] lamport = new long[1];
        final VectorClock[] vector = { new VectorClock(count) };
        final Object lock = new Object();
        final Map<Integer, Set<Long>> seen = new HashMap<>();

        for (int k = 0; k < count; k++) {
            final int partition = k;
            seen.put(partition, new HashSet<>());

            Thread thread = new Thread(() -> {
                try {
                    pubSub.subscribe(Wire.topicData(partition), message -> {
                        try {
                            if (!Wire.type(message).equals("REG")) {
                                throw new IllegalArgumentException("tipo inesperado");
                            }

                            String[] fields = Wire.regParts(message);
                            SensorRecord record = SensorRecord.fromLine(fields[2]);

                            if (record.getSensorId().startsWith("ALARM-")) {
                                return;
                            }

                            long alarmLamport;
                            VectorClock alarmVector;

                            synchronized (lock) {
                                Set<Long> partitionSeen = seen.get(partition);

                                if (!partitionSeen.add(record.getSeq())) {
                                    return;
                                }

                                lamport[0] = Math.max(lamport[0], record.getLamport());
                                vector[0].merge(record.getVector());

                                alarmLamport = lamport[0];
                                alarmVector = vector[0].copy();
                            }

                            if (record.getTemperature() < Wire.LIMIAR_ALARME) {
                                return;
                            }

                            String alarmSensor = "ALARM-" + record.getSensorId();
                            int target = partitioner.of(alarmSensor);

                            pubSub.publish(
                                    Wire.topicIn(target),
                                    Wire.in(alarmSensor, record.getTemperature(),
                                            alarmLamport, alarmVector));

                            System.out.println("ALARME: " + record.getSensorId()
                                    + " p" + partition + "#" + record.getSeq()
                                    + " -> p" + target);
                        } catch (IOException e) {
                            System.out.println("Publicacao/subscricao falhou: " + e.getMessage());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (RuntimeException e) {
                            System.out.println("REJEITADO: " + message);
                        }
                    });
                } catch (IOException e) {
                    System.out.println("Subscricao interrompida: " + e.getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            thread.start();
        }

    }
}