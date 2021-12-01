
import fetch from "node-fetch";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function f(endpoint, request)
{
  try {

    const response = await fetch('https://localhost:7141/' + endpoint, {
      method: 'POST',
      body: JSON.stringify(request),
      headers: { 'Content-Type': 'application/json' }
    })

    const json = await response.json()

    console.log(json.status);
  } catch (error) {
    console.log(error);
  }
}

async function init()
{
  let request = {
    databaseName: "test",
  };

  await f("create-db", request)

  request = {
    databaseName: "test",
    tableName: "my_table",
    columns: [
      { name: "id", type: "id", primary: true },
      { name: "name", type: "string" },
      { name: "code", type: "int", index: "multi" },
      { name: "enabled", type: "bool" },
    ]
  };

  await f("create-table", request)
}

(async () => {
	//for (let i = 0; i < 100; i++)
		//await Promise.all([f(), f(), f(), f(), f(), f(), f(), f(), f()]);

    await init();
    //await f("create-table");
})();
