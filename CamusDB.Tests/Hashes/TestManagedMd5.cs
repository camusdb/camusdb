/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using NUnit.Framework;
using CamusDB.Core.Util.Hashes;

namespace CamusDB.Tests.Hashes;

/// <summary>
/// The browser build computes SQL <c>md5()</c> with <see cref="ManagedMd5"/>, and the suite never runs
/// on that build. These tests are its only coverage: the RFC 1321 vectors, and a byte-for-byte match
/// with the platform MD5 across every tail length that changes the padding (one final block or two).
/// </summary>
public sealed class TestManagedMd5
{
    [TestCase("", "d41d8cd98f00b204e9800998ecf8427e")]
    [TestCase("a", "0cc175b9c0f1b6a831c399e269772661")]
    [TestCase("abc", "900150983cd24fb0d6963f7d28e17f72")]
    [TestCase("message digest", "f96b697d7cb7938d525a2f31aaf161d0")]
    [TestCase("abcdefghijklmnopqrstuvwxyz", "c3fcd3d76192e4007dfb496cca67e13b")]
    [TestCase("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", "d174ab98d277d9f5a5611c2c9f419d9f")]
    [TestCase("12345678901234567890123456789012345678901234567890123456789012345678901234567890", "57edf4a22be3c955ac49da2e2107b67a")]
    public void MatchesRfc1321Vectors(string input, string expectedHex)
    {
        Assert.AreEqual(expectedHex, Hex(Encoding.ASCII.GetBytes(input)));
    }

    [Test]
    public void MatchesPlatformMd5ForEveryLengthUpToFourBlocks()
    {
        Random random = new(20260919);

        for (int length = 0; length <= 64 * 4 + 1; length++)
        {
            byte[] input = new byte[length];
            random.NextBytes(input);

            Assert.AreEqual(Convert.ToHexStringLower(MD5.HashData(input)), Hex(input), $"length {length}");
        }
    }

    [Test]
    public void MatchesPlatformMd5ForALargeInput()
    {
        byte[] input = new byte[1_000_003];
        new Random(7).NextBytes(input);

        Assert.AreEqual(Convert.ToHexStringLower(MD5.HashData(input)), Hex(input));
    }

    [Test]
    public void RejectsADestinationShorterThanTheDigest()
    {
        Assert.Throws<ArgumentException>(() => ManagedMd5.HashData([1, 2, 3], new byte[15]));
    }

    private static string Hex(byte[] input)
    {
        byte[] digest = new byte[ManagedMd5.HashSizeInBytes];
        Assert.AreEqual(ManagedMd5.HashSizeInBytes, ManagedMd5.HashData(input, digest));
        return Convert.ToHexStringLower(digest);
    }
}
