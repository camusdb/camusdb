
import fetch from "node-fetch";

let id = 1000;

/*Null = 0,
    Id = 1,
    Integer = 2,
    String = 3,
    Bool = 4,
    Float = 5,
*/

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function f()
{
  try {

    let request = {
        databaseName: "test",
        tableName: "my_table",
        columns: ["id", "name", "age", "enabled"],
        values: {
          "id": {
            "type": 1, // id
            "value": parseInt(Math.random() * 1000000).toString(),
          },
          "code": {
            "type": 2, // integer
            "value": "1234",
          },
          "name": {
            "type": 3, // string
            "value": "some string".repeat(512),
          },
          "enabled": {
            "type": 4, // bool
            "value": "false",
          }
        }
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
		//await Promise.all([f(), f(), f(), f(), f(), f(), f(), f(), f()]);
    await f();
})();
