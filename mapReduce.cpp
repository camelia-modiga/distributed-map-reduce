#include <iostream>
#include <mpi.h>
#include <fstream>
#include <map>
#include <regex>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <vector>

using namespace std;

enum class TaskType
{
    Map,
    Reduce,
    Close
};

enum class WorkerState
{
    Free,
    Working,
    Done
};

static string inputFolder = "";
static string outputFolder = "";
static string tempFolder = "";

// functie care citeste continutul unui director
vector<string> readContentFromDirectory(string path)
{
    vector<string> v;
    DIR *dir = opendir(path.c_str());
    struct dirent *dp;
    while ((dp = readdir(dir)) != NULL)
    {
        if (strcmp(dp->d_name, ".") != 0 && strcmp(dp->d_name, "..") != 0)
        {
            v.push_back(dp->d_name);
        }
    }
    closedir(dir);
    return v;
}

// functie care creeaza un nou director
void createFolder(string path)
{
    // S_IRWXU = read, write, execute/search by owner
    // S_IRWXO = read, write, execute/search by others
    mkdir(path.c_str(), S_IRWXU | S_IRWXO);
}

// functie care verifica starea worker-ilor
bool checkWorkers(WorkerState *states, int n)
{
    for (int i = 1; i < n; ++i)
        if (states[i] != WorkerState::Free)
            return false;
    return true;
}

// functie care se ocupa cu etapa de mapare
void masterPhase(int n, TaskType task)
{

    int dummyBuffer;
    int flag;
    vector<string> taskList;

    MPI_Request sendReq;
    MPI_Status status;

    switch (task)
    {
    case TaskType::Map:
        cout << "[Master] starting Map phase\n";
        taskList = readContentFromDirectory(inputFolder);
        cout << "[Master] " << taskList.size() << " files found for map phase\n";
        break;
    case TaskType::Reduce:
        cout << "[Master] starting Reduce phase\n";
        taskList = readContentFromDirectory(tempFolder);
        cout << "[Master] " << taskList.size() << " words found for reduce phase\n";
        break;
    }

    // initial toti workerii sunt in starea free
    WorkerState *workerState = new WorkerState[n];
    for (int i = 0; i < n; ++i)
    {
        workerState[i] = WorkerState::Free;
    }

    //vector requesturi neblocante
    MPI_Request *workerRequests = new MPI_Request[n];

    int currentTask = 0;

    while (currentTask < taskList.size() || (!checkWorkers(workerState, n)))
    {
        for (int i = 1; i < n; ++i)
        {
            if (workerState[i] == WorkerState::Free && currentTask < taskList.size())
            {
                MPI_Isend(taskList[currentTask].c_str(), taskList[currentTask].size() + 1, MPI_CHAR, i, (int)task, MPI_COMM_WORLD, &sendReq);
                MPI_Request_free(&sendReq);
                MPI_Irecv(&dummyBuffer, 1, MPI_INT, i, MPI_ANY_TAG, MPI_COMM_WORLD, &(workerRequests[i]));
                // dupa ce ii este atribuit un fisier
                // worker-ul isi schimba starea
                workerState[i] = WorkerState::Working;
                // trecem la task-ul urmator
                currentTask++;
            }

            if (workerState[i] != WorkerState::Free)
            {
                // se testeaza ca requestul a fost finalizat
                MPI_Test(&workerRequests[i], &flag, &status);

                // daca a fost finalizat
                if (flag)
                {
                    // si workerii si-au terminat executia
                    if (status.MPI_TAG == (int)WorkerState::Done)
                    {
                        // workerii trec in starea free
                        workerState[i] = WorkerState::Free;
                    }
                }
            }
        }
    }
    // eliberare de memorie
    free(workerState);
    free(workerRequests);
}

void mapStoreResultPhase(string file)
{
    map<string, int> frequency;

    char word[255];
    char c;
    int poz = 0;

    string sWord;

    ifstream workerCurrentFile(inputFolder + "/" + file);

    // daca fisierul poate fi deschis
    if (!workerCurrentFile.fail())
    {
        while (workerCurrentFile.get(c))
        {
            c = tolower(c);
            if (isdigit(c) || isalpha(c))
            {
                word[poz++] = c;
            }
            else
            {
                if (poz != 0)
                {
                    word[poz] = '\0';
                    poz = 0;
                    ++frequency[word];
                }
            }
        }

        for (auto it = frequency.begin(); it != frequency.end(); ++it)
        {
            // se creeaza in folderul temp un director cu numele cuvantului
            string wordFolder = tempFolder + "/" + it->first;
            createFolder(wordFolder);
            // in folderul care are numele cuvantului se creeaza cate un fisiere text
            // care are numele fisierului din directorul cu datele de intrare
            // si contine numarul de aparitii ale cuvatului
            ofstream outfile(wordFolder + "/" + file);
            outfile << it->second;
        }
    }
}

// functie care preia fisierele din directoarele din folderul temp
void reducePhase(string folder, int rank)
{
    ofstream output;
    // numele fisierului de iesire este reprezentat de cuvantul worker si numarul procesului
    string outFileName = outputFolder + "/" + "worker_" + to_string(rank) + ".txt";

    // se deschide fisierul pentru scriere si adaugare
    output.open(outFileName, ofstream::out | ofstream::app);

    // se citeste continutul fiecarui director din folderul temp
    vector<string> v = readContentFromDirectory(tempFolder + "/" + folder);
    string buffer;

    // rezultatul va fi de forma: {cuvant : fisiere:[ nume_fisier, numar_aparitii] }
    output << "{ cuvant: " << folder << ", fisiere:[";
    int index = 0;
    for (string file : v)
    {
        // ce citesc valorile din fisierele din directorul temp
        ifstream workerCurrentFile(tempFolder + "/" + folder + "/" + file);
        getline(workerCurrentFile, buffer);
        output << "{ nume_fisier: " + file + ", numar_aparitii:" + buffer + "}";
        if (index < v.size() - 1)
        {
            output << ",";
            index++;
        }
    }
    output << "]}\n";
}

void workerPhase(int rank)
{
    bool running = true;
    int message = 0;
    char buffer[255];
    string stringBuffer;

    MPI_Status status;

    while (running)
    {
        MPI_Recv(buffer, 255, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        stringBuffer = string(buffer);

        switch ((TaskType)status.MPI_TAG)
        {
        case TaskType::Close:
            running = false;
            break;

        case TaskType::Map:
            mapStoreResultPhase(buffer);
            MPI_Send(&message, 1, MPI_INT, 0, (int)WorkerState::Done, MPI_COMM_WORLD);
            break;

        case TaskType::Reduce:
            reducePhase(buffer, rank);
            MPI_Send(&message, 1, MPI_INT, 0, (int)WorkerState::Done, MPI_COMM_WORLD);
            break;

        default:
            cout << "Bad task type\n";
            MPI_Finalize();
        }
    }
}

// functie care concateneaza intr-un singur fisier rezultatele obtinute de workeri
void mergeWorkersResults(int n)
{
    // se citeste continutul directorului out
    vector<string> v = readContentFromDirectory(outputFolder);
    vector<string> files;
    regex workerFileRegex("worker_([0-9])+.txt");
    for (string s : v)
    {
        // daca numele fisierului este de forma worker_numarProces
        // acesta este adugat in vectorul files
        if (regex_match(s, workerFileRegex))
        {
            files.push_back(s);
        }
    }

    // fisierul de iesire
    ofstream output;
    // calea fisierului
    string outFileName = outputFolder + "/output.txt";
    // se deschide fisierul pentru scriere
    output.open(outFileName, std::ofstream::out);

    string buffer;
    string aux = "";
    for (string s : files)
    {
        // se citesc pe rand fisierele din vectorul files
        ifstream workerCurrentFile(outputFolder + "/" + s);
        while (getline(workerCurrentFile, buffer))
        {
            aux += buffer + ",";
        }
        output << aux;
        aux = "";
    }
}

int main(int argc, char **argv)
{
    // rank-ul procesului curent
    int myrank;
    // numarul de procese
    int count;

    MPI_Init(&argc, &argv);
    // aflam rank-ul pentru procesul curent
    MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
    // aflam numarul de procese
    MPI_Comm_size(MPI_COMM_WORLD, &count);

    // se verifica numarul de argumente ale liniei de comanda
    if (argc != 3)
    {
        if (myrank == 0)
        {
            printf("Invalid number of input parameters!");
        }

        // se termina aplicatia MPI
        MPI_Finalize();
        return -1;
    }

    // numele directorului care contine datele de intrare si numele directorului care contine datele de iesire
    // sunt date ca argumente ale liniei de comanda
    inputFolder = argv[1];
    outputFolder = argv[2];
    tempFolder = string(argv[2]) + "/temp";

    // se considera nodul master cel de rang 0
    if (myrank == 0)
    {
        // daca exista deja folderul output acesta va fi sters
        system(("rm -rf " + outputFolder).c_str());
        // se creeaza directorul output
        createFolder(outputFolder);
        // se creeaza directorul temp
        createFolder(tempFolder);
        cout << "[Master] Folders cleared and created\n";

        // etapa de mapare
        masterPhase(count, TaskType::Map);
        cout << "[Master] Map stage finished\n";

        // etapa de reducere
        masterPhase(count, TaskType::Reduce);
        cout << "[Master] Reduce stage finished\n";

        // concatenarea rezultatelor
        mergeWorkersResults(count);
        cout << "[Master] Result concatenation finished\n";

        // procesele sunt informate ca se termina etapa de reducere si se ajunge in etapa close
        // char msg='0';
        for (int i = 1; i < count; ++i)
        {
            MPI_Send(nullptr, 0, MPI_BYTE, i, (int)TaskType::Close, MPI_COMM_WORLD);
        }
    }
    else
    {
        workerPhase(myrank);
    }

    // se termina aplicatia MPI
    MPI_Finalize();
    return 0;
}