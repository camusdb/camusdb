/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

// The WebAssembly SDK builds an executable, so there must be an entry point. The page never runs it:
// JavaScript loads the runtime and calls the exports on CamusDB.Wasm.PlaygroundEngine directly.
return 0;
