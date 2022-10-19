// Copyright 2022 Confluent Inc.
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
using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Benchmark
{
    public class BenchmarkConfig
    {
        private readonly TopicSpecification topic;
        private string bootstrapServers;

        public BenchmarkConfig(TopicSpecification topic)
        {
            this.topic = topic;
        }

        public string TopicName => this.topic.Name;

        public string BootstrapServers
        {
            get => bootstrapServers;
            set
            {
                bootstrapServers = value;
                this.SetClients(x => x.BootstrapServers = value);
            }
        }

        public ProducerConfig Producer { get; } = new();

        public ConsumerConfig Consumer { get; } = new() { GroupId = "benchmark-consumer-group" };

        public AdminClientConfig AdminClient { get; } = new();

        public int NumberOfMessages { get; set; } = 5000000;

        public int MessageSize { get; set; } = 100;

        public int HeaderCount { get; set; }

        public int NumberOfTests { get; set; } = 1;

        public void SetUserName(string value)
        {
            this.SetClients(
                client =>
                {
                    client.SaslUsername = value;
                    client.SecurityProtocol = SecurityProtocol.SaslSsl;
                    client.SaslMechanism = SaslMechanism.Plain;
                });
        }

        public void SetPassword(string value)
        {
            this.SetClients(x => x.SaslPassword = value);
        }

        private void SetClients(Action<ClientConfig> action)
        {
            action(this.Producer);
            action(this.Consumer);
            action(this.AdminClient);
        }
    }
}
