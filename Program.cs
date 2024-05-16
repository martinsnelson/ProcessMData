using Amazon.S3.Model;
using Amazon.S3;
using Amazon;
using System.Diagnostics;

Console.WriteLine("Escolha um tipo de ação");

await Main("s3"); //read, write, s3

static async Task Main(string args)
{
    #region[Read file]
    if (args.Contains("read")) {
        // Defina o caminho do arquivo onde você deseja escrever a mensagens
        string docPath1 = Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory);

        /**/
        // Defina o caminho do arquivo de entrada
        string caminhoArquivoEntrada = "bigfile.txt";

        // Determine o número máximo de threads disponíveis
        int maxThreads1 = Process.GetCurrentProcess().Threads.Count;

        // Buffers para armazenar partes lidas por cada thread
        List<List<string>> buffers1 = new List<List<string>>(maxThreads1);
        for (int i = 0; i < maxThreads1; i++)
        {
            buffers1.Add(new List<string>());
        }

        // Use Stopwatch para medir o tempo decorrido
        Stopwatch stopwatch1 = Stopwatch.StartNew();

        // Leia todas as linhas do arquivo de entrada
        string[] linhas = await File.ReadAllLinesAsync(Path.Combine(docPath1, caminhoArquivoEntrada));

        // Divida o trabalho de leitura entre threads
        Parallel.For(0, linhas.Length, new ParallelOptions { MaxDegreeOfParallelism = maxThreads1 }, i =>
        {
            // Determine qual buffer usar com base no índice atual
            int threadIndex = i % maxThreads1;

            // Adicione a linha ao buffer correspondente
            lock (buffers1[threadIndex])
            {
                buffers1[threadIndex].Add(linhas[i]);
            }
        });

        // Pare o cronômetro para medir o tempo de execução do loop
        stopwatch1.Stop();
        Console.WriteLine($"Tempo para ler o arquivo e adicionar às buffers: {stopwatch1.ElapsedMilliseconds} ms");

        stopwatch1.Restart();
        // Processar os dados armazenados em buffers
        // (Por exemplo, você pode querer combinar ou processar os dados conforme necessário)
        Console.WriteLine("Início do processamento de buffers...");
        foreach (var buffer in buffers1)
        {
            // Processar cada buffer
            foreach (var linha in buffer)
            {
                // Por exemplo, exiba a linha ou processe-a conforme necessário
                Console.WriteLine(linha);
            }
        }

        // Pare o cronômetro novamente
        stopwatch1.Stop();
        Console.WriteLine($"Processamento de buffers concluído.: {stopwatch1.ElapsedMilliseconds} ms");
        return;
        #endregion
    }

    #region[Write file]

    if (args.Contains("write"))
    {

        // Crie uma instância de Stopwatch
        Stopwatch stopwatch = new Stopwatch();

        stopwatch.Start();

        // Defina o caminho do arquivo onde você deseja escrever a mensagens
        string docPath = Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory);

        // Defina o caminho do arquivo onde você deseja escrever a mensagem
        string nomeFile = "bigfile.txt";

        // Defina a quantidade de vezes que você deseja escrever a mensagem
        int n = 1000000;

        int maxThreads = Process.GetCurrentProcess().Threads.Count; // ou int maxThreads = 12;

        List<List<string>> buffers = new List<List<string>>(maxThreads);
        for (int i = 0; i < maxThreads; i++)
        {
            buffers.Add(new List<string>());
        }

        Parallel.For(0, n, new ParallelOptions { MaxDegreeOfParallelism = maxThreads }, i =>
        {
            // Defina a mensagem que você deseja escrever
            string mensagem = $"{Guid.NewGuid()}";
            //string mensagem = $"{i}";

            int threadIndex = i % maxThreads;

            lock (buffers[threadIndex])
            {
                buffers[threadIndex].Add(mensagem);
            }
        });

        // Pare o cronômetro para medir o tempo de execução do loop
        stopwatch.Stop();
        Console.WriteLine($"Tempo para adicionar mensagens aos buffers: {stopwatch.ElapsedMilliseconds} ms");

        // Recomece o cronômetro para medir o tempo de escrita no arquivo
        stopwatch.Restart();

        // Abra o arquivo em modo de escrita, e agora, combine os buffers e escreva os dados em um arquivo
        using (StreamWriter escritor = new StreamWriter(Path.Combine(docPath, nomeFile)))
        {
            foreach (var buffer in buffers)
            {
                foreach (var line in buffer)
                {
                    await escritor.WriteLineAsync(line);
                }
            }
        }

        // Pare o cronômetro novamente
        stopwatch.Stop();
        Console.WriteLine($"Tempo para escrever no arquivo: {stopwatch.ElapsedMilliseconds} ms");

        Console.WriteLine($"As mensagens foram escritas em '{docPath}' usando paralelismo.");


        Console.WriteLine($"As mensagens foram escritas em '{nomeFile}'.");
        Console.WriteLine($"Tempo decorrido: {stopwatch.ElapsedMilliseconds} milissegundos");

        return;
    }
    #endregion

    #region[Carregar Big file no S3]

    if (args.Contains("s3")) 
    {
        Stopwatch stopwatchs3 = new Stopwatch();
        stopwatchs3.Start();
    
        string bucketName = "bucket-processm-data";
        string keyName = "bigfile.txt";
        //string filePath = "caminho/para/o/arquivo.txt";
        string filePath = Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory);

            // Crie um cliente S3
            AmazonS3Client s3Client = new AmazonS3Client(RegionEndpoint.SAEast1); // Especifique a região correta

        try
        {
            // Carregue o arquivo
            using (FileStream fileStream = new FileStream(Path.Combine(filePath, keyName), FileMode.Open, FileAccess.Read))
            {
                PutObjectRequest request = new PutObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName,
                    InputStream = fileStream,
                    ContentType = "text/plain"
                };

                // Envie o arquivo para S3
                PutObjectResponse response = await s3Client.PutObjectAsync(request);
                Console.WriteLine($"Arquivo {keyName} enviado para bucket {bucketName} com sucesso!");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao enviar arquivo para S3: {ex.Message}");
        }

        // Pare o cronômetro novamente
        stopwatchs3.Stop();
        Console.WriteLine($"Tempo para enviar arquivo para o S3 : {stopwatchs3.ElapsedMilliseconds} ms");
        return;
    }
            #endregion

            #region[Exemplo 1 WriteAsync file]
            // Loop para escrever a mensagem 'n' vezes
            //for (int i = 0; i < n; i++)
            //{
            //    count++;
            //    // Defina a mensagem que você deseja escrever
            //    string mensagem = $"{Guid.NewGuid()}";

            //    // Use WriteAsync para escrever a mensagem de forma assíncrona
            //    await escritor.WriteAsync(mensagem);
            //    //Task.Delay(10);

            //    // Adicione uma nova linha após cada escrita
            //    await escritor.WriteLineAsync();

            //    Console.WriteLine($"Guid número {count}");
            //}
            #endregion
            #region[Exemplo 2 write file]
            /**/
            // Set a variable to the Documents path.
            //string docPath = Environment.GetFolderPath(Environment.SpecialFolder.DesktopDirectory);

            //// Write the specified text asynchronously to a new file named "WriteTextAsync.txt".
            //using (StreamWriter outputFile = new StreamWriter(Path.Combine(docPath, "WriteTextAsync.txt")))
            //{

            //    await outputFile.WriteAsync("This is a sentence. \nThis is a sentence two");
            //}
            #endregion
            #region[Exemplo 3 write file]
            /* */
            // Create a string array with the lines of text
            //string[] lines = { "First line", "Second line", "Third line" };

            //// Set a variable to the Documents path.
            //string docPath2 =Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);

            //// Write the string array to a new file named "WriteLines.txt".
            //using (StreamWriter outputFile = new StreamWriter(Path.Combine(docPath2, "WriteLines.txt")))
            //{
            //    foreach (string line in lines)
            //        outputFile.WriteLine(line);
            //}
            #endregion
}

Console.ReadKey();
