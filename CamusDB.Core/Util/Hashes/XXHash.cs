
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.Util.Hashes;

/**
 * XXHash (https://cyan4973.github.io/xxHash/)
 */
public static class XXHash
{
	private const uint PRIME32_1 = 2654435761U;
	private const uint PRIME32_2 = 2246822519U;
	private const uint PRIME32_3 = 3266489917U;
	private const uint PRIME32_4 = 668265263U;
	private const uint PRIME32_5 = 374761393U;

	public static uint Compute(byte[] buf, int index = 0, int len = -1, uint seed = 0)
	{
		uint h32;

		if (len == -1)
			len = buf.Length;

		if (len >= 16)
		{
			int limit = len - 16;
			uint v1 = seed + PRIME32_1 + PRIME32_2;
			uint v2 = seed + PRIME32_2;
			uint v3 = seed + 0;
			uint v4 = seed - PRIME32_1;

			do
			{
				v1 = CalcSubHash(v1, buf, index);
				index += 4;
				v2 = CalcSubHash(v2, buf, index);
				index += 4;
				v3 = CalcSubHash(v3, buf, index);
				index += 4;
				v4 = CalcSubHash(v4, buf, index);
				index += 4;
			} while (index <= limit);

			h32 = RotateLeft(v1, 1) + RotateLeft(v2, 7) + RotateLeft(v3, 12) + RotateLeft(v4, 18);
		}
		else
		{
			h32 = seed + PRIME32_5;
		}

		h32 += (uint)len;

		while (index <= len - 4)
		{
			h32 += BitConverter.ToUInt32(buf, index) * PRIME32_3;
			h32 = RotateLeft(h32, 17) * PRIME32_4;
			index += 4;
		}

		while (index < len)
		{
			h32 += buf[index] * PRIME32_5;
			h32 = RotateLeft(h32, 11) * PRIME32_1;
			index++;
		}

		h32 ^= h32 >> 15;
		h32 *= PRIME32_2;
		h32 ^= h32 >> 13;
		h32 *= PRIME32_3;
		h32 ^= h32 >> 16;

		return h32;
	}

	private static uint CalcSubHash(uint value, byte[] buf, int index)
	{
		uint read_value = BitConverter.ToUInt32(buf, index);
		value += read_value * PRIME32_2;
		value = RotateLeft(value, 13);
		value *= PRIME32_1;
		return value;
	}

	private static uint RotateLeft(uint value, int count)
	{
		return (value << count) | (value >> (32 - count));
	}
}
