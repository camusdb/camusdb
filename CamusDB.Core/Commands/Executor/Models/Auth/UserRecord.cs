/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Core.CommandsExecutor.Models;

/// <summary>
/// A server-level database user, persisted in the shared <c>_system/auth/user:{normalizedName}</c>
/// keyspace by <see cref="Controllers.AuthCatalog"/>. Cross-database — not scoped to any one database.
///
/// <para><see cref="Name"/> is stored case-preserving for display; the KV key and all lookups use the
/// normalized (lower-cased) form. The two epochs are the revocation levers used in the enforcement
/// phase: <see cref="CredentialEpoch"/> advances on password change / drop / revoke-all (invalidating
/// issued tokens), and <see cref="AuthorizationEpoch"/> advances on every grant/revoke (forcing a
/// privilege reload). They live on the user so a single record read tells a validator whether a cached
/// token or privilege snapshot is still current.</para>
/// </summary>
public sealed class UserRecord
{
    /// <summary>
    /// Immutable identity, allocated once when the user is created and never reused.
    ///
    /// <para>The name is not an identity: it is droppable and re-creatable, so anything that stores
    /// "who owns this" by name would silently transfer to whoever next claims the name. A view's owner
    /// is stored as this id for exactly that reason. Null on records written before the field existed;
    /// such an owner cannot be verified and is therefore treated as absent rather than as matching.
    /// </para>
    /// </summary>
    public string? Id { get; set; }

    /// <summary>User-visible name in its original case.</summary>
    public string Name { get; set; } = "";

    /// <summary>Password verifier, or <c>null</c> when the user has no password and cannot authenticate.</summary>
    public Credential? Credential { get; set; }

    /// <summary>Advances when the credential is rotated or the user dropped; invalidates issued tokens.</summary>
    public long CredentialEpoch { get; set; }

    /// <summary>Advances on every grant/revoke affecting this user; forces a privilege reload.</summary>
    public long AuthorizationEpoch { get; set; }

    /// <summary>
    /// True for the bootstrap superuser: bypasses every privilege check and is the only identity
    /// allowed to administer users/grants. Deliberately a distinct attribute, not an <c>ALL</c> grant,
    /// so it cannot be conferred by <c>GRANT</c> — it is set only by the one-time bootstrap.
    /// </summary>
    public bool IsSuperuser { get; set; }

    /// <summary>UTC timestamp the user was created.</summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>Returns a field-complete copy so a mutation path never accidentally drops a field.</summary>
    public UserRecord Copy() => new()
    {
        Id = Id,
        Name = Name,
        Credential = Credential is null ? null : new Credential
        {
            Algorithm = Credential.Algorithm,
            Salt = Credential.Salt,
            Hash = Credential.Hash,
            Iterations = Credential.Iterations,
        },
        CredentialEpoch = CredentialEpoch,
        AuthorizationEpoch = AuthorizationEpoch,
        IsSuperuser = IsSuperuser,
        CreatedAt = CreatedAt,
    };
}
