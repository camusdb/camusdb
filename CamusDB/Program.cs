
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core;
using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;

//Console.WriteLine(UInt32.MaxValue);

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();

builder.Services.AddSingleton<CommandExecutor>();
builder.Services.AddSingleton<CommandValidator>();
builder.Services.AddSingleton<CatalogsManager>();

WebApplication app = builder.Build();

// Initialize DB system
CamusStartup camus = new(
    app.Services.GetRequiredService<CommandExecutor>()
);

await camus.Initialize(await File.ReadAllTextAsync("Config/config.yml"));

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

// app.Add

app.UseHttpsRedirection();
app.UseStaticFiles();

app.MapControllers();

app.UseRouting();

app.UseAuthorization();

app.MapRazorPages();

app.Run();
