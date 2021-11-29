
import fetch from "node-fetch";

let id = 1000;

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function f()
{
  try {

    let request = {
        databaseName: "test",
        tableName: "my_table",
        columns: ["id", "name", "age", "enabled"],
        values: [
          {
            "type": 0, // id
            "value": "1501",
          },
          {
            "type": 1, // integer
            "value": "1234",
          },
          {
            "type": 3, // string
            "value": "some string",
          },
          {
            "type": 4, // bool
            "value": "false",
          }
        ]
    };

    const response = await fetch('https://localhost:7141/insert', {
        method: 'POST',
        body: JSON.stringify(request),
        headers: { 'Content-Type': 'application/json' }
    })

    const json = await response.json()

    console.log(json.status);
    //console.log(json.explanation);
  } catch (error) {
    console.log(error);
  }
}

(async () => {
	//for (let i = 0; i < 100; i++)
		await Promise.all([f(), f(), f(), f(), f(), f(), f(), f(), f()]);
})();
