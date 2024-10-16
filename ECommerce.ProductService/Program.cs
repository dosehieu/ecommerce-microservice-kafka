using Ecommerce.Common;
using ECommerce.ProductService.Data;
using ECommerce.Services.ProductService.Kafka;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddDbContext<ProductDbContext>(options =>
    options.UseSqlServer("Data Source=.\\sqlexpress; Initial Catalog=EcommerceProduct; Integrated Security=True; TrustServerCertificate=True"));

builder.Services.AddSingleton<IKafkaProducer, KafkaProducer>();
builder.Services.AddHostedService<ProductConsumer>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
