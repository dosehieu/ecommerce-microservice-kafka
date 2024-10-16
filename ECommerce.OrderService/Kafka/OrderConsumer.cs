using Confluent.Kafka;
using Ecommerce.Common;
using Ecommerce.Model;
using ECommerce.OrderService.Data;
using Newtonsoft.Json;

namespace ECommerce.Services.OrderService.Kafka
{
    public class OrderConsumer(IServiceProvider serviceProvider) : KafkaConsumer(topics)
    {
        private static readonly string[] topics = ["payment-processed", 
            "products-reservation-failed",
            "products-reservation-canceled"];
        private OrderDbContext GetDbContext()
        {
            var scope = serviceProvider.CreateScope();
            return scope.ServiceProvider.GetRequiredService<OrderDbContext>();
        }
        protected override async Task ConsumeAsync(ConsumeResult<string, string> consumeResult)
        {
            await base.ConsumeAsync(consumeResult);
            switch (consumeResult.Topic)
            {
                case "payment-processed":
                    await HandleComfirmOrder(consumeResult.Message.Value);
                    break;
                case "products-reservation-failed":
                    await HandleCancelOrder(consumeResult.Message.Value);
                    break;
                case "products-reservation-canceled":
                    await HandleCancelOrder(consumeResult.Message.Value);
                    break;
            }
        }

        public async Task HandleComfirmOrder(string message)
        {
            var orderMessage = JsonConvert.DeserializeObject<OrderMessage>(message);
            using var dbContext = GetDbContext();
            var order = await dbContext.Orders.FindAsync(orderMessage.OrderId);
            if (order != null)
            {
                order.Status = "Comfirm";
                await dbContext.SaveChangesAsync();
            }
        }

        public async Task HandleCancelOrder(string message)
        {
            var orderMessage = JsonConvert.DeserializeObject<OrderMessage>(message);
            using var dbContext = GetDbContext();
            var order = await dbContext.Orders.FindAsync(orderMessage.OrderId);
            if (order != null)
            {
                order.Status = "Cancel";
                await dbContext.SaveChangesAsync();
            }
        }
    }
}
