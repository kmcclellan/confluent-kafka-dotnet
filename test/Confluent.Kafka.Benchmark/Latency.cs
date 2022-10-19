// Copyright 2020-2022 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.IO;


namespace Confluent.Kafka.Benchmark
{
    public class Latency
    {
        public static void Run(BenchmarkConfig config, int messagesPerSecond)
        {
            if (!Stopwatch.IsHighResolution)
            {
                Console.WriteLine("WARNING: High precision timing is not supported on this platform.");
            }

            if (messagesPerSecond < 1000)
            {
                throw new Exception("Message rate must be >= 1000 msg/s");
            }

            var sw = new Stopwatch();
            sw.Start();

            var monitorObj = new Object();

            var consumerTask = Task.Run(() => {
                // Use middle results only to better estimate steady state performance.
                var trimStart = (long)(config.NumberOfMessages * 0.05);
                var trimEnd = (long)(config.NumberOfMessages * 0.95);
                var results = new long[trimEnd - trimStart];

                config.Consumer.SessionTimeoutMs = 6000;
                config.Consumer.ConsumeResultFields = config.HeaderCount == 0 ? "none" : "headers";
                config.Consumer.QueuedMinMessages = 1000000;
                config.Consumer.AutoOffsetReset = AutoOffsetReset.Latest;
                config.Consumer.EnableAutoCommit = true;

                using (var consumer = new ConsumerBuilder<Null, byte[]>(config.Consumer)
                    .SetPartitionsAssignedHandler((c, partitions) => {
                        // Ensure there is no race between consumer determining start offsets and production starting.
                        var initialAssignment = partitions.Select(p => new TopicPartitionOffset(p, c.QueryWatermarkOffsets(p, TimeSpan.FromSeconds(5)).High)).ToList();
                        if (initialAssignment.Where(p => p.Offset != 0).Count() > 0)
                        {
                            Console.WriteLine("Start offsets: [" + String.Join(", ", initialAssignment.OrderBy(a => (int)a.Partition).Select(a => a.Offset.ToString())) + "]");
                        }
                        lock (monitorObj) { Monitor.Pulse(monitorObj); }
                        return initialAssignment;
                    })
                    .Build())
                {
                    consumer.Subscribe(config.TopicName);

                    var count = 0;
                    while (count < config.NumberOfMessages)
                    {
                        var cr = consumer.Consume(1000);
                        if (cr == null)
                        {
                            if (count > 0) { Console.WriteLine($"No message consumed after {count} consumed"); }
                            continue;
                        }
                        if (count < trimStart || count >= trimEnd) { count += 1; continue; }

                        long writeMilliseconds;
                        using (var s = new MemoryStream(cr.Message.Value))
                        using (var br = new BinaryReader(s))
                        {
                            writeMilliseconds = br.ReadInt64();
                        }
                        long elapsedMilliSeconds;
                        lock (sw) { elapsedMilliSeconds = sw.ElapsedMilliseconds; }
                        long latencyMilliSeconds = elapsedMilliSeconds - writeMilliseconds;

                        results[count++ - trimStart] = latencyMilliSeconds;
                        if (count % (config.NumberOfMessages / 10) == 0)
                        {
                            Console.WriteLine($"...{(count / (config.NumberOfMessages/10))}0% complete");
                        }
                    }

                    Console.WriteLine("done");
                    consumer.Close();
                }

                Array.Sort(results);

                Console.WriteLine(
                    "Latency percentiles (ms) [p50: {0}, p75: {1}, p90: {2}, p95: {3}, p99: {4}]",
                    results[(int)(results.Length * 50.0/100.0)],
                    results[(int)(results.Length * 75.0/100.0)],
                    results[(int)(results.Length * 90.0/100.0)],
                    results[(int)(results.Length * 95.0/100.0)],
                    results[(int)(results.Length * 99.0/100.0)]);
            });


            var producerTask = Task.Run(() => {

                lock (monitorObj) { Monitor.Wait(monitorObj); }

                config.Producer.QueueBufferingMaxMessages = 2000000;
                config.Producer.MessageSendMaxRetries = 3;
                config.Producer.RetryBackoffMs = 500;
                config.Producer.LingerMs = 5;
                config.Producer.DeliveryReportFields = "none";
                config.Producer.EnableIdempotence = true;

                Headers headers = null;
                if (config.HeaderCount > 0)
                {
                    headers = new Headers();
                    for (int i = 0; i < config.HeaderCount; ++i)
                    {
                        headers.Add($"header-{i+1}", new byte[] { (byte)i, (byte)(i+1), (byte)(i+2), (byte)(i+3) });
                    }
                }
                
                using (var producer = new ProducerBuilder<Null, byte[]>(config.Producer).Build())
                {
                    var startMilliseconds = sw.ElapsedMilliseconds;

                    for (int i = 0; i < config.NumberOfMessages; ++i)
                    {
                        var payload = new byte[config.MessageSize];

                        long elapsedMilliseconds;
                        lock (sw) { elapsedMilliseconds = sw.ElapsedMilliseconds; }
                        using (var s = new MemoryStream(payload))
                        using (var bw = new BinaryWriter(s))
                        {
                            bw.Write(elapsedMilliseconds);
                        }

                        producer.Produce(
                            config.TopicName,
                            new Message<Null, byte[]> { Value = payload, Headers = headers },
                            dr =>
                            {
                                if (dr.Error.Code != ErrorCode.NoError)
                                    Console.WriteLine("Message delivery failed: " + dr.Error.Reason);
                            });

                        var desiredProduceCount = (elapsedMilliseconds - startMilliseconds)/1000.0 * messagesPerSecond;

                        // Simple, but about as good as we can do assuming a fast enough rate, and a poor Thread.Sleep precision.
                        if (i > desiredProduceCount)
                        {
                            Thread.Sleep(1);
                        }
                    }

                    while (producer.Flush(TimeSpan.FromSeconds(1)) > 0);

                    long elapsedMilliSeconds;
                    lock (sw) {elapsedMilliSeconds = sw.ElapsedMilliseconds; }

                    Console.WriteLine(
                        "Actual throughput: "
                            + (int)Math.Round(
                                config.NumberOfMessages / ((elapsedMilliSeconds - startMilliseconds) / 1000.0))
                            + " msg/s");
                }
            });

            Task.WaitAll(new [] { producerTask, consumerTask });
        }
    }
}
