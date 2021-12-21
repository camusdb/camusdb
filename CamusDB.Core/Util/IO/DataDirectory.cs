


/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */ namespace CamusDB.Core.Util.IO;

public static class DataDirectory
{
    public static List<FileInfo> GetFiles(string path, string type)
    {
        DirectoryInfo directory = new(path);

        List<FileInfo> filteredFiles = new();

        FileInfo[] files = directory.GetFiles();

        int length = type.Length;

        for (int i = 0; i < files.Length; i++)
        {
            FileInfo file = files[i];

            if (file.Name.Length >= length && file.Name[..length] == type)
                filteredFiles.Add(file);
        }

        filteredFiles.Sort(FileNameComparer);
        
        return filteredFiles;
    }

    private static int FileNameComparer(FileInfo a, FileInfo b)
    {
        return a.FullName.CompareTo(b.FullName);
    }

    public static string GetNextFile(List<FileInfo> files, string type)
    {
        int max = -1;

        foreach (FileInfo file in files)
        {
            if (int.TryParse(file.Name.Replace(type, ""), out int fileId))
            {
                if (fileId > max)
                    max = fileId;
            }
        }

        return (max + 1).ToString("000");
    }
}
