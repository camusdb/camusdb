/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Extensions;

// Fixture.GoodV2 compiles these same sources with GOOD_V2 defined: the same extension, a newer
// version, for the update and rolling-upgrade tests.
#if GOOD_V2
[assembly: CamusExtension("good", "2.0.0")]
#else
[assembly: CamusExtension("good", "1.0.0")]
#endif
