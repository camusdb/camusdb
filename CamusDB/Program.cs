
using CamusDB.Library.Catalogs;
using CamusDB.Library.CommandsExecutor;
using CamusDB.Library.Util;
using CodeExMachina;

var b = new BTree();

b.Put(10, "hello 1");
b.Put(5, "hello 2");
b.Put(18, "hello 3");
b.Put(29, "hello 4");

//Console.WriteLine(b.Get(18));

foreach (Entry entry in b.Traverse())
{
    Console.WriteLine("{0} {1}", entry.key, entry.val);
}

/*var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddRazorPages();

builder.Services.AddSingleton<CommandExecutor>();
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

app.Run();*/
