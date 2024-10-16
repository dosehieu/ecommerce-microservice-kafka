using Confluent.Kafka;
using Ecommerce.Common;
using Ecommerce.Model;
using ECommerce.ProductService.Data;
using Newtonsoft.Json;

namespace ECommerce.Services.ProductService.Kafka
{
    public class ProductConsumer(IServiceProvider serviceProvider, IKafkaProducer kafkaProducer) : KafkaConsumer(topics)
    {
        private static readonly string[] topics = ["order-created", "payment-failed"];
        private ProductDbContext GetDbContext()
        {
            var scope = serviceProvider.CreateScope();
            return scope.ServiceProvider.GetRequiredService<ProductDbContext>();
        }
        protected override async Task ConsumeAsync(ConsumeResult<string, string> consumeResult)
        {
            await base.ConsumeAsync(consumeResult);

            switch (consumeResult.Topic)
            {
                case "order-created":
                    await HandleOrderCreated(consumeResult.Message.Value);
                    break;
                case "payment-failed":
                    await HandlePaymentFailed(consumeResult.Message.Value);
                    break;
            }
        }

        public async Task HandlePaymentFailed(string message)
        {
            var orderMessage = JsonConvert.DeserializeObject<OrderMessage>(message);

            using var dbContext = GetDbContext();
            var product = await dbContext.Products.FindAsync(orderMessage.ProductId);
            if (product != null)
            {
                product.Quantity += orderMessage.Quantity;
                await dbContext.SaveChangesAsync();
            }
            await kafkaProducer.ProduceAsync("products-reservation-canceled", orderMessage);
        }

        public async Task HandleOrderCreated(string message)
        {
            var orderMessage = JsonConvert.DeserializeObject<OrderMessage>(message);
            var isReserved = await ReserveProducts(orderMessage);
            if (isReserved)
            {
                await kafkaProducer.ProduceAsync("products-reserved", orderMessage);
            }
            else
            {
                await kafkaProducer.ProduceAsync("products-reservation-failed", orderMessage);
            }
        }

        public async Task<bool> ReserveProducts(OrderMessage orderMessage)
        {
            using var dbContext = GetDbContext();
            var product = await dbContext.Products.FindAsync(orderMessage.ProductId);
            if (product != null && product.Quantity >= orderMessage.Quantity)
            {
                product.Quantity -= orderMessage.Quantity;
                await dbContext.SaveChangesAsync();
                return true;
            }
            return false;
        }
    }
}
