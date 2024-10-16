using Confluent.Kafka;
using Ecommerce.Common;
using Ecommerce.Model;
using Newtonsoft.Json;

namespace Ecommerce.Services.OrchestratorService.Kafka
{
    public class OrchestratorConsumer(IKafkaProducer kafkaProducer) : KafkaConsumer(topics)
    {
        private static readonly string[] topics = [
            "order-created",
            "products-reserved",
            "products-reservation-failed",
            "payment-processed",
            "payment-failed"
            ];

        protected override async Task ConsumeAsync(ConsumeResult<string, string> consumeResult)
        {
            await base.ConsumeAsync(consumeResult);

            var message = JsonConvert.DeserializeObject<OrderMessage>(consumeResult.Message.Value);
            switch (consumeResult.Topic)
            {
                case "order-created":
                    await kafkaProducer.ProduceAsync("products-reserve", message);
                    break;
                case "products-reserved":
                    await kafkaProducer.ProduceAsync("payment-process", message);
                    break;
                case "products-reservation-failed":
                    await kafkaProducer.ProduceAsync("order-cancel", message);
                    break;
                case "payment-processed":
                    await kafkaProducer.ProduceAsync("order-confirm", message);
                    break;
                case "payment-failed":
                    await kafkaProducer.ProduceAsync("products-cancel-reserve", message);
                    await kafkaProducer.ProduceAsync("order-cancel", message);
                    break;
            }
        }
    }
}
