
import fetch from "node-fetch";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0"

async function f(endpoint)
{
  try {

    const response = await fetch('https://localhost:7141/' + endpoint)
    const json = await response.json()

    console.log(json.url);
    console.log(json.explanation);
  } catch (error) {
    console.log(error);
  }
}

(async () => {
	//for (let i = 0; i < 100; i++)
		//await Promise.all([f(), f(), f(), f(), f(), f(), f(), f(), f()]);

    await f("create-db");
    await f("create-table");
})();
