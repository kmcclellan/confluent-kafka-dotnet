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
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Mono.Options;
using Confluent.Kafka.Admin;


namespace Confluent.Kafka.Benchmark
{
    public class Program
    {

        private static void CreateTopic(TopicSpecification topic, BenchmarkConfig config)
        {
            using (var adminClient = new AdminClientBuilder(config.AdminClient).Build())
            {
                try
                {
                    adminClient.DeleteTopicsAsync(new List<string> { topic.Name }).Wait();
                }
                catch (AggregateException ae)
                {
                    if (!(ae.InnerException is DeleteTopicsException) ||
                        (((DeleteTopicsException)ae.InnerException).Results.Select(r => r.Error.Code).Where(el => el != ErrorCode.UnknownTopicOrPart).Count() > 0))
                    {
                        throw new Exception($"Unable to delete topic {topic.Name}", ae);
                    }
                }

                // Give the cluster a chance to remove the topic. If this isn't long enough (unlikely), there will be an error and the user can just re-run.
                Thread.Sleep(2000);

                try
                {
                    adminClient.CreateTopicsAsync(new List<TopicSpecification> { topic }).Wait();
                }
                catch (AggregateException e)
                {
                    Console.WriteLine("Failed to create topic: " + e.InnerException.Message);
                }
            }
        }

        public static void Main(string[] args)
        {
            bool showHelp = false;
            string mode = null;
            int? messagesPerSecond = null;

            TopicSpecification topic = new()
            {
                Name = "dotnet-benchmark",
                ReplicationFactor = 3
            };

            BenchmarkConfig config = new(topic)
            {
                BootstrapServers = "localhost:9092",
            };

            OptionSet p = new OptionSet
            {
                { "m|mode=", "throughput|latency", m => mode = m },
                { "b|brokers=", $"bootstrap.servers (default: {config.BootstrapServers})", v => config.BootstrapServers = v },
                { "t=", $"topic (default: {topic.Name})", t => topic.Name = t },
                { "g=", $"consumer group (default: {config.Consumer.GroupId})", g => config.Consumer.GroupId = g },
                { "h=", $"number of headers (default: {config.HeaderCount})", (int h) => config.HeaderCount = h },
                { "n=", $"number of messages (default: {config.NumberOfMessages})", (int n) => config.NumberOfMessages = n },
                { "r=", "rate - messages per second (latency mode only). must be > 1000", (int r) => messagesPerSecond = r },
                { "s=", $"message size (default: {config.MessageSize})" , (int s) => config.MessageSize = s },
                { "p=", "(re)create topic with this partition count (default: not set)", (int v) => topic.NumPartitions = v },
                { "f=", $"replication factor when creating topic (default {topic.ReplicationFactor})" , (short f) => topic.ReplicationFactor = f },
                { "u=", "SASL username (will also set protocol=SASL_SSL, mechanism=PLAIN)", config.SetUserName },
                { "w=", "SASL password", config.SetPassword },
                { "help", "show this message and exit", v => showHelp = v != null },
            };

            p.Parse(args);

            if (mode == null || showHelp ||
                (messagesPerSecond != null && mode == "throughput") ||
                (messagesPerSecond == null && mode == "latency"))
            {
                Console.WriteLine("Usage:");
                p.WriteOptionDescriptions(Console.Out);
                return;
            }

            if (topic.NumPartitions > 0)
            {
                CreateTopic(topic, config);
            }

            if (mode == "throughput")
            {
                BenchmarkProducer.TaskProduce(config);
                var firstMessageOffset = BenchmarkProducer.DeliveryHandlerProduce(config);
                BenchmarkConsumer.Consume(config, firstMessageOffset);
            }
            else if (mode == "latency")
            {
                Latency.Run(config, messagesPerSecond.Value);
            }
        }
    }
}
