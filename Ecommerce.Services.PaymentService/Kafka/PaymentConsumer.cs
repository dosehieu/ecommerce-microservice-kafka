using Confluent.Kafka;
using Ecommerce.Common;
using Ecommerce.Model;
using Newtonsoft.Json;

namespace Ecommerce.Services.PaymentService.Kafka
{
    public class PaymentConsumer(IKafkaProducer kafkaProducer) : KafkaConsumer(topics)
    {
        private static readonly string[] topics = ["products-reserved"];

        protected override async Task ConsumeAsync(ConsumeResult<string, string> consumeResult)
        {
            await base.ConsumeAsync(consumeResult);
            switch (consumeResult.Topic)
            {
                case "products-reserved":
                    await HandleProductsReserved(consumeResult.Message.Value);
                    break;
            }
        }

        public async Task HandleProductsReserved(string message)
        {
            var orderMessage = JsonConvert.DeserializeObject<OrderMessage>(message);

            var isPaymentProcessed = ProcessPayment(orderMessage);

            if (isPaymentProcessed)
            {
                await kafkaProducer.ProduceAsync("payment-processed", orderMessage);
            }
            else
            {
                await kafkaProducer.ProduceAsync("payment-failed", orderMessage);
            }
        }

        public bool ProcessPayment(OrderMessage orderMessage)
        {
            // Logic to process payment
            return false;
        }
    }
}
