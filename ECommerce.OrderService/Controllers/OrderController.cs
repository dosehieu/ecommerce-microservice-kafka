using Confluent.Kafka;
using Ecommerce.Common;
using Ecommerce.Model;
using ECommerce.OrderService.Data;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using System.Text.Json;

namespace ECommerce.OrderService.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrderController(OrderDbContext context, IKafkaProducer producer) : ControllerBase
    {
        [HttpPost]
        public async Task<ActionResult<OrderModel>> PostOrder(OrderModel order)
        {
            order.OrderDate = DateTime.Now;
            order.Status = "Pending";
            context.Orders.Add(order);
            await context.SaveChangesAsync();

            var orderMessage = new OrderMessage
            {
                OrderId = order.Id,
                ProductId = order.ProductId,
                Quantity = order.Quantity
            };

            await producer.ProduceAsync("order-created", orderMessage);

            return order;
        }

        [HttpGet]
        public async Task<ActionResult<List<OrderModel>>> GetOrder()
        {
            return await context.Orders.ToListAsync();
        }
    }
}
