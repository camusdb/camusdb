
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Catalogs;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Util;

/*byte[] d = new byte[] { 10, 20, 30, 40 };

ReadOnlyMemory<byte> p = d.AsMemory().Slice(0, 2);

ReadOnlySpan<byte> s = p.Span;

Console.WriteLine(s[0]);
Console.WriteLine(s[1]);

d[0] = 200;
d[1] = 100;

Console.WriteLine(s[0]);
Console.WriteLine(s[1]);

s = p.Span;

Console.WriteLine(s[0]);
Console.WriteLine(s[1]);*/

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();

builder.Services.AddSingleton<CommandExecutor>();
builder.Services.AddSingleton<CommandValidator>();
builder.Services.AddSingleton<CatalogsManager>();

var app = builder.Build();

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
