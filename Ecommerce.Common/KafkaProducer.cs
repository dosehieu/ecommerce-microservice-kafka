using Confluent.Kafka;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Ecommerce.Common
{
    public interface IKafkaProducer
    {
        Task ProduceAsync(string topic, object message);
    }
    public class KafkaProducer: IKafkaProducer
    {
        private readonly IProducer<string, string> _producer;
        public KafkaProducer()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
            };
            _producer = new ProducerBuilder<string, string>(config).Build();
        }
        public async Task ProduceAsync(string topic, object message)
        {
            var kafkaMessage = new Message<string, string> { Value = JsonConvert.SerializeObject(message) };
            await _producer.ProduceAsync(topic, kafkaMessage);
        }
    }
}
