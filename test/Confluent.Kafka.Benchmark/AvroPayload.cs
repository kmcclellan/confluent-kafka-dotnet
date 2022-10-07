using Avro;
using Avro.Specific;

namespace Confluent.Kafka.Benchmark
{
    internal class AvroPayload : ISpecificRecord
    {
        public static Schema _SCHEMA = Schema.Parse(@"{""type"":""record"",""name"":""AvroPayload"",""namespace"":""Confluent.Kafka.Benchmark"",""fields"":[{""name"":""Data"",""type"":""bytes""}]}");

        public Schema Schema => _SCHEMA;

        public byte[] Data { get; set; }

        public object Get(int fieldPos)
        {
            return fieldPos switch
            {
                0 => this.Data,
                _ => throw new AvroRuntimeException(),
            };
        }

        public void Put(int fieldPos, object fieldValue)
        {
            switch (fieldPos)
            {
                case 0:
                    this.Data = (byte[])fieldValue;
                    break;

                default: throw new AvroRuntimeException();
            }
        }
    }

}
