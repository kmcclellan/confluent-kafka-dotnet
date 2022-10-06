// Copyright 2016-2022 Confluent Inc.
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
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System.Linq;
using Confluent.Kafka.Benchmark.Payloads;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;


namespace Confluent.Kafka.Benchmark
{
    public static class BenchmarkProducer
    {
        private static long BenchmarkProducerImpl(BenchmarkConfig config, bool useDeliveryHandler)
        {
            if (config.SchemaType == null)
            {
                return BenchmarkProducerImpl(config, useDeliveryHandler, x => x);
            }

            var schemaClient = new CachedSchemaRegistryClient(config.SchemaRegistry);

            return config.SchemaType switch
            {
                SchemaType.Json => BenchmarkProducerImpl(
                    config,
                    useDeliveryHandler,
                    x => new JsonPayload { Data = x },
                    new JsonSerializer<JsonPayload>(schemaClient)),

                _ => throw new NotSupportedException()
            };
        }

        private static long BenchmarkProducerImpl<TPayload>(
            BenchmarkConfig config,
            bool useDeliveryHandler,
            Func<string, TPayload> payloadFactory,
            IAsyncSerializer<TPayload> serializer = null)
        {
            // mirrors the librdkafka performance test example.
            config.Producer.QueueBufferingMaxMessages = 2000000;
            config.Producer.MessageSendMaxRetries = 3;
            config.Producer.RetryBackoffMs = 500;
            config.Producer.LingerMs = 100;
            config.Producer.DeliveryReportFields = "none";

            DeliveryResult<Null, TPayload> firstDeliveryReport = null;

            Headers headers = null;
            if (config.HeaderCount > 0)
            {
                headers = new Headers();
                for (int i = 0; i < config.HeaderCount; ++i)
                {
                    headers.Add($"header-{i+1}", new byte[] { (byte)i, (byte)(i+1), (byte)(i+2), (byte)(i+3) });
                }
            }

            var producerBuilder = new ProducerBuilder<Null, TPayload>(config.Producer);

            if (serializer != null)
            {
                if (useDeliveryHandler)
                {
                    producerBuilder.SetValueSerializer(serializer.AsSyncOverAsync());
                }
                else
                {
                    producerBuilder.SetValueSerializer(serializer);
                }
            }

            using (var producer = producerBuilder.Build())
            {
                for (var j = 0; j < config.NumberOfTests; j += 1)
                {
                    Console.WriteLine(
                        $"{producer.Name} producing on {config.TopicName} "
                            + (useDeliveryHandler ? "[Action<Message>]" : "[Task]"));

                    var val = payloadFactory(string.Concat(Enumerable.Repeat("abcdefgjij", config.MessageSize / 10)));

                    // this avoids including connection setup, topic creation time, etc.. in result.
                    firstDeliveryReport = producer.ProduceAsync(
                        config.TopicName,
                        new Message<Null, TPayload> { Value = val, Headers = headers })
                        .Result;

                    var startTime = DateTime.Now.Ticks;

                    if (useDeliveryHandler)
                    {
                        var autoEvent = new AutoResetEvent(false);
                        var msgCount = config.NumberOfMessages;
                        Action<DeliveryReport<Null, TPayload>> deliveryHandler = (DeliveryReport<Null, TPayload> deliveryReport) => 
                        {
                            if (deliveryReport.Error.IsError)
                            {
                                // Not interested in benchmark results in the (unlikely) event there is an error.
                                Console.WriteLine($"A error occured producing a message: {deliveryReport.Error.Reason}");
                                Environment.Exit(1); // note: exceptions do not currently propagate to calling code from a deliveryHandler method.
                            }

                            if (--msgCount == 0)
                            {
                                autoEvent.Set();
                            }
                        };

                        for (int i = 0; i < config.NumberOfMessages; i += 1)
                        {
                            try
                            {
                                producer.Produce(
                                    config.TopicName,
                                    new Message<Null, TPayload> { Value = val, Headers = headers },
                                    deliveryHandler);
                            }
                            catch (ProduceException<Null, TPayload> ex)
                            {
                                if (ex.Error.Code == ErrorCode.Local_QueueFull)
                                {
                                    producer.Poll(TimeSpan.FromSeconds(1));
                                    i -= 1;
                                }
                                else
                                {
                                    throw;
                                }
                            }
                        }

                        while (true)
                        {
                            if (autoEvent.WaitOne(TimeSpan.FromSeconds(1)))
                            {
                                break;
                            }
                            Console.WriteLine(msgCount);
                        }
                    }
                    else
                    {
                        try
                        {
                            var tasks = new Task[config.NumberOfMessages];
                            for (int i = 0; i < config.NumberOfMessages; i += 1)
                            {
                                tasks[i] = producer.ProduceAsync(
                                    config.TopicName,
                                    new Message<Null, TPayload> { Value = val, Headers = headers });

                                if (tasks[i].IsFaulted)
                                {
                                    if (((ProduceException<Null, TPayload>)tasks[i].Exception.InnerException).Error.Code == ErrorCode.Local_QueueFull)
                                    {
                                        producer.Poll(TimeSpan.FromSeconds(1));
                                        i -= 1;
                                    }
                                    else
                                    {
                                        // unexpected, abort benchmark test.
                                        throw tasks[i].Exception;
                                    }
                                }
                            }

                            Task.WaitAll(tasks);
                        }
                        catch (AggregateException ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
                    }

                    var duration = DateTime.Now.Ticks - startTime;

                    Console.WriteLine($"Produced {config.NumberOfMessages} messages in {duration/10000.0:F0}ms");
                    Console.WriteLine($"{config.NumberOfMessages / (duration/10000.0):F0}k msg/s");

                    File.AppendAllText(
                        Path.Join(AppContext.BaseDirectory, "output.csv"),
                        $"produce-{(useDeliveryHandler ? "callback" : "task")},{config.SchemaType},"
                            + $"{config.NumberOfMessages},{duration}{Environment.NewLine}");
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            }

            return firstDeliveryReport.Offset;
        }

        /// <summary>
        ///     Producer benchmark masquerading as an integration test.
        ///     Uses Task based produce method.
        /// </summary>
        public static long TaskProduce(BenchmarkConfig config) => BenchmarkProducerImpl(config, false);

        /// <summary>
        ///     Producer benchmark (with custom delivery handler) masquerading
        ///     as an integration test. Uses Task based produce method.
        /// </summary>
        public static long DeliveryHandlerProduce(BenchmarkConfig config) => BenchmarkProducerImpl(config, true);
    }
}
