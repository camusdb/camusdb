
import crypto from "crypto";

let PROCESS_UNIQUE = null;
let index = Math.floor(Math.random() * 0xffffff);

function getInc()
{
    return (index = (index + 1) % 0xffffff);
}

export function getObjectIdValue()
{
  let time = Math.floor(Date.now() / 1000);

  const inc = getInc();
  const buffer = Buffer.alloc(12);

  // 4-byte timestamp
  buffer.writeUInt32BE(time, 0);

  // set PROCESS_UNIQUE if yet not initialized
  if (PROCESS_UNIQUE === null) {
    PROCESS_UNIQUE = crypto.randomBytes(5);
  }

  // 5-byte process unique
  buffer[4] = PROCESS_UNIQUE[0];
  buffer[5] = PROCESS_UNIQUE[1];
  buffer[6] = PROCESS_UNIQUE[2];
  buffer[7] = PROCESS_UNIQUE[3];
  buffer[8] = PROCESS_UNIQUE[4];

  // 3-byte counter
  buffer[11] = inc & 0xff;
  buffer[10] = (inc >> 8) & 0xff;
  buffer[9] = (inc >> 16) & 0xff;

  return buffer.toString('hex');
}

//exports.getObjectIdValue = getObjectIdValue;