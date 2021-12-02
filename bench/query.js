
import fetch from "node-fetch";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function f()
{
  try {

    let request = {
        databaseName: "test",
        tableName: "my_table",
    };

    const response = await fetch('https://localhost:7141/query', {
        method: 'POST',
        body: JSON.stringify(request),
        headers: { 'Content-Type': 'application/json' }
    })

    const json = await response.json()

    console.log(json.status);

    for (let i = 0; i < json.rows.length; i++)
        console.log(json.rows[i])

  } catch (error) {
    console.log(error);
  }
}

(async () => {
	//for (let i = 0; i < 100; i++)
		//await Promise.all([f(), f(), f(), f(), f(), f(), f(), f(), f()]);
    await f();
})();
