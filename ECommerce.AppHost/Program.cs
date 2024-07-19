var builder = DistributedApplication.CreateBuilder(args);
var apiProductService = builder.AddProject<Projects.ECommerce_Services_ProductService>("apiservice-product");

var apiOrderService = builder.AddProject<Projects.ECommerce_Services_OrderService>("apiservice-order");

builder.AddProject<Projects.ECommerce_Client>("webfrontend")
    .WithReference(apiProductService)
    .WithReference(apiOrderService);



builder.Build().Run();
