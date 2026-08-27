/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDB.Workload.Client;
using CamusDB.Workload.Workload;

namespace CamusDB.Workload.Operations;

/// <summary>
/// A parameterized primary-key point read. It runs as a genuine read-only autocommit statement (the
/// read connection defaults to <c>ReadOnly</c> mode), fully consumes the single-row result, and
/// validates that the returned id and row shape match what was requested before the operation is
/// counted complete — a read that silently returned the wrong row or no row is a correctness failure,
/// not a fast success. There is one constant SQL text per table and the id is bound as a parameter, so
/// the server's parse/plan caches see one statement per table, not one per row.
/// </summary>
public sealed class ReadOperation
{
    private readonly ConnectionSet _connections;
    private readonly Dataset _dataset;

    /// <summary>One prepared-shape SELECT per dataset table, indexed the same way as
    /// <see cref="Dataset.TableNames"/>, so the read path never builds SQL per operation.</summary>
    private readonly string[] _sqlByTable;

    public ReadOperation(ConnectionSet connections, Dataset dataset)
    {
        _connections = connections;
        _dataset = dataset;
        _sqlByTable = new string[dataset.TableNames.Count];
        for (int i = 0; i < _sqlByTable.Length; i++)
            _sqlByTable[i] = $"SELECT id, balance, version, payload FROM {dataset.TableNames[i]} WHERE id = @id";
    }

    public async Task<OperationResult> ExecuteAsync(long rowIndex, CancellationToken ct)
    {
        (string id, _, _, _) = _dataset.RowFor(rowIndex);
        CamusConnection conn = _connections.NextRead();

        try
        {
            using CamusCommand cmd = conn.CreateSelectCommand(_sqlByTable[_dataset.TableIndexOf(rowIndex)]);
            cmd.Parameters.Add("@id", ColumnType.Id, id);

            using CamusDataReader reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return OperationResult.Failure(OperationKind.Read, OperationStatus.InternalError, "READ_MISSING_ROW");

            if (reader.FieldCount != 4)
                return OperationResult.Failure(OperationKind.Read, OperationStatus.InternalError, "READ_BAD_SHAPE");

            string readId = reader.GetString(0);
            if (!string.Equals(readId, id, StringComparison.Ordinal))
                return OperationResult.Failure(OperationKind.Read, OperationStatus.InternalError, "READ_WRONG_ID");

            // Touch the remaining columns so the whole row is materialized before we call it done.
            _ = reader.GetInt64(1);
            _ = reader.GetInt64(2);
            _ = reader.GetString(3);

            return OperationResult.ReadOk();
        }
        catch (Exception ex)
        {
            (OperationStatus status, string code) = ErrorClassifier.Classify(ex);
            return OperationResult.Failure(OperationKind.Read, status, code);
        }
    }
}
