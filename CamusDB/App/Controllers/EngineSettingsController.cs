/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.App.Models;
using CamusDB.App.Services;
using CamusDB.Core.Storage.Kv;
using Microsoft.AspNetCore.Mvc;

namespace CamusDB.App.Controllers;

/// <summary>
/// Reports the engine settings this node resolved, so a benchmark can record what a measurement
/// actually ran under rather than what a configuration file happened to mention.
///
/// <para><c>SHOW VARIABLES</c> cannot answer this. It reports the configuration layer, so an engine
/// key nobody set reads as unset there — while CamusDB's mode-specific baseline underneath it is
/// where the durability knobs actually come from.</para>
///
/// <para>Unauthenticated, on the same footing as <c>/ping</c>, <c>/v1/version</c> and
/// <c>/v1/cluster/health</c>. The response is operational configuration, not data, and the one
/// credential-shaped option is redacted. Keep it on a trusted interface like the rest of that family.</para>
/// </summary>
[ApiController]
public sealed class EngineSettingsController : ControllerBase
{
    private readonly EmbeddedKahuna kahuna;

    public EngineSettingsController(EmbeddedKahuna kahuna) => this.kahuna = kahuna;

    [HttpGet]
    [Route("/v1/engine-settings")]
    public JsonResult GetEngineSettings() => new(new EngineSettingsResponse(EngineSettingsReader.Describe(kahuna.Options)));
}
