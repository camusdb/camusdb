
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;
using CamusDB.Core.Util.Trees;
using System.Threading.Tasks;
using System.Collections.Generic;
using CamusDB.Core.Catalogs;
using CamusDB.Core.Catalogs.Models;
using CamusDB.Core.CommandsExecutor;
using CamusDB.Core.CommandsExecutor.Models;
using CamusDB.Core.CommandsExecutor.Models.Tickets;
using CamusDB.Core.CommandsValidator;
using CamusDB.Core.Util.ObjectIds;
using CsvHelper;
using System;
using System.Globalization;
using System.IO;
using CsvHelper.Configuration;

namespace CamusDB.Tests.Integration;

public class TestIntegration
{
    private async Task<(string, CommandExecutor)> SetupDatabase()
    {
        string dbname = "github12";

        CommandValidator validator = new();
        CatalogsManager catalogsManager = new();
        CommandExecutor executor = new(validator, catalogsManager);

        /*CreateDatabaseTicket databaseTicket = new(
            name: dbname
        );

        await executor.CreateDatabase(databaseTicket);*/

        return (dbname, executor);
    }

    /*
     * 
     * CREATE TABLE jobs (
  id STRING(32) NOT NULL,
  branch STRING(64) NOT NULL,
  jobType INT64 NOT NULL,
  author STRING(64) NOT NULL,
  message STRING(256) NOT NULL,
  commit STRING(40) NOT NULL,
  platform INT64 NOT NULL,
  priority INT64 NOT NULL,
  createdAt INT64 NOT NULL,
  updatedAt INT64 NOT NULL,
  groupKey STRING(40),
  status INT64 NOT NULL,
  startedAt INT64 DEFAULT (0),
  completedAt INT64 DEFAULT (0),
  runId STRING(32),
) PRIMARY KEY(id);
     */

    private async Task<(string, CommandExecutor)> SetupBasicTable()
    {
        (string dbname, CommandExecutor executor) = await SetupDatabase();

        /*CreateTableTicket tableTicket = new(
            database: dbname,
            name: "jobs_two",
            new ColumnInfo[]
            {
                new ColumnInfo("id", ColumnType.Id, primary: true),
                new ColumnInfo("branch", ColumnType.String, notNull: true),
                new ColumnInfo("jobType", ColumnType.Integer64, notNull: true),
                new ColumnInfo("author", ColumnType.String, notNull: true),
                new ColumnInfo("message", ColumnType.String, notNull: true),
                new ColumnInfo("commit", ColumnType.String, notNull: true),
                new ColumnInfo("platform", ColumnType.Integer64, notNull: true),
                new ColumnInfo("priority", ColumnType.Integer64, notNull: true),
                new ColumnInfo("createdAt", ColumnType.Integer64, notNull: true),
                new ColumnInfo("updatedAt", ColumnType.Integer64, notNull: true),
                new ColumnInfo("groupKey", ColumnType.String, notNull: true),
                new ColumnInfo("status", ColumnType.Integer64, notNull: true),
                new ColumnInfo("startedAt", ColumnType.Integer64, notNull: true),
                new ColumnInfo("completedAt", ColumnType.Integer64, notNull: true),
                new ColumnInfo("runId", ColumnType.String, notNull: true),
            }
        );

        await executor.CreateTable(tableTicket);*/

        return (dbname, executor);
    }

    [Test]
    public async Task TestEmpty()
    {
        (string dbname, CommandExecutor executor) = await SetupBasicTable();

        var conf = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            //HeaderValidated = null,
            //MissingFieldFound = null
        };

        using (var reader = new StreamReader("/tmp/jobs6.csv"))
        using (var csvReader = new CsvReader(reader, conf))
        {
            IEnumerable<JobsRecord> records = csvReader.GetRecords<JobsRecord>();

            foreach (JobsRecord record in records)
            {
                //Console.WriteLine(record);

                await executor.Insert(new(
                    database: dbname,
                    name: "jobs_two",
                    values: new Dictionary<string, ColumnValue>()
                    {
                        { "id", new ColumnValue(ColumnType.Id, ObjectIdGenerator.Generate().ToString()) },
                        { "branch", new ColumnValue(ColumnType.String, record.Branch!.Trim('"')) },
                        { "jobType", new ColumnValue(ColumnType.Integer64, record.JobType!.ToString()) },
                        { "author", new ColumnValue(ColumnType.String, record.Author!.Trim('"')) },
                        { "message", new ColumnValue(ColumnType.String, record.Message!.Trim('"')) },
                        { "commit", new ColumnValue(ColumnType.String, record.Commit!.Trim('"')) },
                        { "platform", new ColumnValue(ColumnType.Integer64, record.Platform!.ToString()) },
                        { "priority", new ColumnValue(ColumnType.Integer64, record.Priority!.ToString()) },
                        { "createdAt", new ColumnValue(ColumnType.Integer64,record.CreatedAt!.ToString()) },
                        { "updatedAt", new ColumnValue(ColumnType.Integer64, record.UpdatedAt!.ToString()) },
                        { "groupKey", new ColumnValue(ColumnType.String, record.GroupKey!.Trim('"')) },
                        { "status", new ColumnValue(ColumnType.Integer64, record.Status!.ToString()) },
                        { "startedAt", new ColumnValue(ColumnType.Integer64, record.StartedAt!.ToString()) },
                        { "completedAt", new ColumnValue(ColumnType.Integer64, record.CompletedAt!.ToString()) },
                        { "runId", new ColumnValue(ColumnType.String, record.RunId!.Trim('"')) },
                    }
                ));

            }

            // Console.WriteLine(records);
        }

        /*QueryTicket queryTicket = new(
           database: dbname,
           name: "jobs",
           index: null,
           filters: null
        );

        int count = 0;

        await foreach (Dictionary<string, ColumnValue> result in await executor.Query(queryTicket))
        {
            //Console.WriteLine(result["id"].Value);
            //Console.WriteLine(result["branch"].Value);
            //Console.WriteLine(result["author"].Value);

            count++;
        }

        Console.WriteLine(count);

        QueryTicket queryTicket = new(
           database: dbname,
           name: "jobs",
           index: null,
           filters: new()
           {
               new("branch", "=", new ColumnValue(ColumnType.String, "main"))
           }
        );

        int count = 0;

        await foreach (Dictionary<string, ColumnValue> result in await executor.Query(queryTicket))
        {
            //Console.WriteLine(result["id"].Value);
            //Console.WriteLine(result["branch"].Value);
            //Console.WriteLine(result["author"].Value);

            count++;
        }

        Console.WriteLine(count);

        var xx = await executor.OpenDatabase(dbname);
        Console.WriteLine(xx.TableSpace.NumberPages);

        await Task.Delay(60000);*/
    }
}

public class JobsRecord
{
    public string? JobId { get; set; }

    public string? Branch { get; set; }

    public int JobType { get; set; }

    public string? Author { get; set; }

    public string? Message { get; set; }

    public string? Commit { get; set; }

    public int Platform { get; set; }

    public int Priority { get; set; }

    public int CreatedAt { get; set; }

    public int UpdatedAt { get; set; }

    public string? GroupKey { get; set; }

    public int Status { get; set; }

    public int StartedAt { get; set; }

    public int CompletedAt { get; set; }

    public string? RunId { get; set; }
}