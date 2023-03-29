using System;
using System.Collections.Generic;
using System.IO;

namespace SongPlayAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            // Specify the path of the input file
            string inputFile = @"/Users/batuhanalpkurban/Downloads/exhibitA-input.csv";

            // Read the input file and store the data in a list
            List<string[]> data = new List<string[]>();
            using (StreamReader reader = new StreamReader(inputFile))
            {
                while (!reader.EndOfStream)
                {
                    string line = reader.ReadLine();
                    string[] fields = line.Split('\t');
                    data.Add(fields);
                }
            }

            // Calculate the number of distinct plays and unique clients for each song
            Dictionary<string, HashSet<string>> clientSongCount = new Dictionary<string, HashSet<string>>();
            foreach (string[] row in data)
            {
                string songId = row[1];
                string clientId = row[2];
                
                // Count the number of unique clients that played each song
                if (clientSongCount.ContainsKey(clientId))
                {
                    clientSongCount[clientId].Add(songId);
                }
                else
                {
                    HashSet<string> clients = new HashSet<string>();
                    clients.Add(clientId);
                    clientSongCount[clientId] = clients;
                }
            }


            Dictionary<int, int> distinctPlayCount = new Dictionary<int, int>();

            foreach (string clientId in clientSongCount.Keys)
            {
                int count = clientSongCount[clientId].Count;
                if (distinctPlayCount.ContainsKey(count))
                {
                    distinctPlayCount[count]++;
                }
                else
                {
                    distinctPlayCount[count] = 1;
                }
            }

            // Write the output to a tab-delimited file
            string outputFile = @"/Users/batuhanalpkurban/Downloads/exhibitA-output.csv";
            using (StreamWriter writer = new StreamWriter(outputFile))
            {
                writer.WriteLine("DISTINCT_PLAY_COUNT\tCLIENT_COUNT");

                foreach (int playCount in distinctPlayCount.Keys)
                {
                    int clientCount = distinctPlayCount[playCount];
                    writer.WriteLine(playCount + "\t" + clientCount);
                }
            }
        }
    }
}