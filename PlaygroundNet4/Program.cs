using System;
using System.Collections.Concurrent;
using System.Diagnostics;

internal class Program
{
    static void Main(string[] args)
    {
        var threads = Process.GetCurrentProcess().Threads;
        var tid = AppDomain.GetCurrentThreadId();
        foreach (ProcessThread t in threads)
        {
            if (t.Id == tid)
            {
                Console.WriteLine("User Time " + t.UserProcessorTime.ToString(@"d\:hh\:mm\:ss"));
                Console.WriteLine("Kernel Time " + t.PrivilegedProcessorTime.TotalSeconds.ToString("F0") + " sec");
            }
        }

        Console.ReadKey();
    }
}
