using Ecommerce.Model;
using Microsoft.EntityFrameworkCore;
using System.Reflection.Metadata;

namespace ECommerce.ProductService.Data
{
    public class ProductDbContext : DbContext
    {
        public ProductDbContext(DbContextOptions<ProductDbContext> options): base(options)
        {
            Database.EnsureCreated();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ProductModel>().HasData(new ProductModel { Id = 1, Name = "Shirt", Quantity = 100, Price = 20 });
            modelBuilder.Entity<ProductModel>().HasData(new ProductModel { Id = 2, Name = "Pant", Quantity = 100, Price = 50 });
            modelBuilder.Entity<ProductModel>().HasData(new ProductModel { Id = 3, Name = "Polo", Quantity = 100, Price = 100 });
            base.OnModelCreating(modelBuilder);
        }
        public DbSet<ProductModel> Products { get; set; }
    }
}
