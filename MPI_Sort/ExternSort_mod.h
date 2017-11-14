#ifndef EXTERN_SORT_MOD_H
#define EXTERN_SORT_MOD_H
#include <cassert>

#include <stdio.h>
#include <stdlib.h>     // for rand, etc.
#include <string.h>     // for memcpy
#include <time.h>       // for time(NULL)
#include <mpi.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <sstream>
#include <iostream>
#include <string>

#define  INIT  1        // Message giving size and height
#define  DATA  2        // Message giving vector to sort
#define  ANSW  3        // Message returning sorted vector
#define  FINI  4        // Send permission to terminate

#define  BUFSIZE  1024        // Send permission to terminate
#define MAX_PATH 1024

class ExternSort
{
public:
  void sort()
  {
    char* fileName = m_in_file;
    struct stat buf;
    int result;
    result = stat( fileName, &buf );
    long file_count = 0;

    if(S_IFDIR & buf.st_mode){
      printf("##folder\n");
      char * pFilePath = fileName;
      DIR * dir;
      struct dirent * ptr;
      struct stat stStatBuf;
      dir = opendir(pFilePath);
      if(dir == NULL){
        printf("ERROR: Can't open dir \"%s\"\n",pFilePath);
        return;
      } else {
        while ((ptr = readdir(dir)) != NULL){
          char Path[MAX_PATH];
          strcpy(Path, pFilePath);
          strncat(Path, "/", 1);
          strcat(Path, ptr->d_name);
          if (stat(Path, &stStatBuf) == -1){
            printf("Get the stat error on file:%s\n", ptr->d_name);
            continue;
          }
          if (stStatBuf.st_mode & S_IFREG){
            printf("process file　%s\n", Path);
            file_count = memory_sort(Path,file_count);
          }
        }
        closedir(dir);
      }
    }else if(S_IFREG & buf.st_mode){
      printf("###file\n");
      file_count = memory_sort(fileName,file_count);
    }
    int myRank, rc;
    rc = MPI_Comm_rank (MPI_COMM_WORLD, &myRank);
    if ( myRank == 0 )        // Host process
      merge_sort(file_count);
  }

  ExternSort(const char *input_file, const char * out_file, long count)
  {
    m_count = count;

    m_in_file = new char[strlen(input_file) + 1];
    strcpy(m_in_file, input_file);

    m_out_file = new char[strlen(out_file) + 1];
    strcpy(m_out_file, out_file);
  }
  virtual ~ExternSort()
  {
    delete [] m_in_file;
    delete [] m_out_file;
  }

private:
  long m_count; 
  char *m_in_file;
  char *m_out_file; 
  unsigned long file_part_thr;
protected:
  long read_data(int f, long a[], long n)
  {
    long i = 0;
    int bytes_read;
    char line[BUFSIZE];
    while (bytes_read = read(f, line, BUFSIZE-1) && file_part_thr > 0 && i<n)
    {
      if ((bytes_read == -1) && (errno != EINTR)) break;
      else if (bytes_read > 0) {

        if (line[strlen(line) - 1] == '\n')
          line[strlen(line)-1] = '\0';
        //char* new_line = new char[strlen(line) + 1];
        //strcpy(new_line, line);
        if (line!=NULL)
        {
          //      std::string str(line);

          std::istringstream stream(line);
          while(stream>>a[i])
            i++;
          file_part_thr--;
        }
      }
    }
    printf("read to:%lu \n", file_part_thr);
#ifdef DEBUG
    printf("read:%d integer\n", i);
#endif
    return i;
  }
  void write_data(FILE* f, long a[], long n)
  {
    for(int i = 0; i < n; ++i)
      fprintf(f, "%li ", a[i]);
  }
  char* temp_filename(int index)
  {
    char *tempfile = new char[100];
    sprintf(tempfile, "temp%d.txt", index);
    return tempfile;
  }
  static int cmp_int(const void *a, const void *b)
  {
    return *(int*)a - *(int*)b;
  }

  void my_error(const char* errstring, int line)
  {
    fprintf(stderr,"line:%d",line);
    perror(errstring);
    exit(1);
  }
  unsigned long get_file_size(int fd)
  {
    unsigned long file_size;

    if((lseek(fd,0,SEEK_END)) == -1)
    {
      my_error("lseek",__LINE__);
    }
    if((file_size  = lseek(fd,0,SEEK_CUR)) == -1)
    {
      my_error("lseek",__LINE__);
    }
    if((lseek(fd,0,SEEK_SET)) == -1)
    {
      my_error("lseek",__LINE__);
    }
    return file_size;
  }

  int mpi_sort(long *vector, long vec_size)
  {
    int myRank, nProc;
    int rc;
    long size = vec_size;          // Size of the vector being sorted
#ifdef DEBUG
    std::cout<<"vec_size: "<<vec_size<<std::endl;
#endif
    rc = MPI_Comm_rank (MPI_COMM_WORLD, &myRank);
    rc = MPI_Comm_size (MPI_COMM_WORLD, &nProc);
    if ( myRank == 0 )        // Host process
    {
      int rootHt = 0, nodeCount = 1;

      while ( nodeCount < nProc )
      {  nodeCount += nodeCount; rootHt++;  }

      printf ("%d processes mandates root height of %d\n", nProc, rootHt);
      parallelMerge ( vector, size, rootHt);
    }
    else                      // Node process
    {
      int   iVect[2],        // Message sent as an array
      height,          // Pulled from iVect
      parent;          // Computed from myRank and height
      MPI_Status status;     // required by MPI_Recv

      rc = MPI_Recv( iVect, 2, MPI_INT, MPI_ANY_SOURCE, INIT,
          MPI_COMM_WORLD, &status );
      size   = iVect[0];     // Isolate size
      height = iVect[1];     // and height
      vector = (long*) calloc (size, sizeof *vector);

      rc = MPI_Recv( vector, size, MPI_LONG, MPI_ANY_SOURCE, DATA,
          MPI_COMM_WORLD, &status );

      parallelMerge ( vector, size, height );

#ifdef DEBUG
      printf ("%d resigning from MPI\n", myRank); fflush(stdout);
#endif
      free(vector);
      return 0;
    }
#ifdef DEBUG
    printf ("%d resigning from MPI\n", myRank); fflush(stdout);
#endif
  }


  static int compare ( const void* left, const void* right )
  {
    long *lt = (long*) left,
        *rt = (long*) right,
        diff = *lt - *rt;

    if ( diff < 0 ) return -1;
    if ( diff > 0 ) return +1;
    return 0;
  }
  /**
   * Parallel merge logic under MPI
   *
   * The working core:  each internal node ships its right-hand
   * side to the proper node below it in the processing tree.  It
   * recurses on this function to process the left-hand side, as
   * the node one closer to the leaf level.
   */
  void parallelMerge ( long *vector, int size, int myHeight )
  {
    int parent;
    int myRank, nProc;
    int rc, nxt, rtChild;

    rc = MPI_Comm_rank (MPI_COMM_WORLD, &myRank);
    rc = MPI_Comm_size (MPI_COMM_WORLD, &nProc);

    parent = myRank & ~(1<<myHeight);
    nxt = myHeight - 1;
    if ( nxt >= 0 )
      rtChild = myRank | ( 1 << nxt );

#ifdef DEBUG
    if ( myHeight > 0 )
      printf ("%#x -> %#x -> %#x among %d\n", parent, myRank, rtChild, nProc);
#endif
    if ( myHeight > 0 )
    {
      //Possibly a half-full node in the processing tree
      if ( rtChild >= nProc )     // No right child.  Move down one level
        parallelMerge ( vector, size, nxt );
      else
      {
        int   left_size  = size / 2,
            right_size = size - left_size;
        long *leftArray  = (long*) calloc (left_size, sizeof *leftArray),
            *rightArray = (long*) calloc (right_size, sizeof *rightArray);
        int   iVect[2];
        int   i, j, k;                // Used in the merge logic
        MPI_Status status;            // Return status from MPI

        memcpy (leftArray, vector, left_size*sizeof *leftArray);
        memcpy (rightArray, vector+left_size, right_size*sizeof *rightArray);
        if(rightArray==NULL)
          return;
#ifdef DEBUG
        printf ("%d sending data to %d\n", myRank, rtChild); fflush(stdout);
#endif
        iVect[0] = right_size;
        iVect[1] = nxt;
        rc = MPI_Send( iVect, 2, MPI_INT, rtChild, INIT,
            MPI_COMM_WORLD);
        rc = MPI_Send( rightArray, right_size, MPI_LONG, rtChild, DATA,
            MPI_COMM_WORLD);

        parallelMerge ( leftArray, left_size, nxt );
#ifdef DEBUG
        printf ("%d waiting for data from %d\n", myRank, rtChild); fflush(stdout);
#endif
        rc = MPI_Recv( rightArray, right_size, MPI_LONG, rtChild, ANSW,
            MPI_COMM_WORLD, &status );

        // Merge the two results back into vector
        i = j = k = 0;
        while ( i < left_size && j < right_size )
          if ( leftArray[i] > rightArray[j])
            vector[k++] = rightArray[j++];
          else
            vector[k++] = leftArray[i++];
        while ( i < left_size )
          vector[k++] = leftArray[i++];
        while ( j < right_size )
          vector[k++] = rightArray[j++];
        free( leftArray);
        free( rightArray);
      }
    }
    else
    {
      qsort( vector, size, sizeof *vector, compare );
#ifdef DEBUG
      printf ("%d leaf sorting %d items.\n", myRank, size); fflush(stdout);
#endif
    }

    /**
     * Note:  If the computed parent is different from myRank, then
     * this is a right-hand side and needs to be sent as a message
     * back to its parent.  Otherwise, the "communication" is done
     * automatically because the result is generated in place.
     */
    if ( parent != myRank )
      rc = MPI_Send( vector, size, MPI_LONG, parent, ANSW,
          MPI_COMM_WORLD );
  }


  long memory_sort(const char *inputFileName,long &file_count)
  {

    int myRank, nProc ,rc;
    int counter = 0;
    rc = MPI_Comm_rank (MPI_COMM_WORLD, &myRank);
    rc = MPI_Comm_size (MPI_COMM_WORLD, &nProc);

    if(myRank==0){
  
      int fin;
      if ((fin = open(inputFileName, O_RDONLY|O_LARGEFILE)) == -1) {
        fprintf(stderr, "Open %s Error：%s\n", inputFileName, strerror(errno));
        exit(1);
      }
      char *fileName = temp_filename(file_count++);
      FILE *tempFile = fopen(fileName, "w");
  
      unsigned long file_size = 0;
      file_size = get_file_size(fin);
      file_part_thr = file_size  / BUFSIZE ;
      long n_slave = 1; //start
      long n_master = 1;
      long *array_master = new long[m_count];
      long *array_slave = new long[m_count];

      while((n_slave>0) ){
//        printf("-----------\n n is:%d\n----------\n", n_slave);
        if(( n_slave = read_data(fin, array_slave, m_count)) > 0) {
          counter++;
          for(int i=1; i<nProc; i++){
             MPI_Send(&n_slave, 1, MPI_LONG, i, 0, MPI_COMM_WORLD); 
             MPI_Send(array_slave, n_slave, MPI_LONG, i, 0, MPI_COMM_WORLD);          
          }
          mpi_sort(array_slave, n_slave);
          write_data(tempFile, array_slave, n_slave);
//          printf("counter: %d, rank:%d, size: %li, m_count: %li\n",counter, myRank, n_slave, m_count);
//          printf("--------------------------->\n");

        }
      }

      n_slave = 0;
      for(int i=1 ;i< nProc; i++){
        MPI_Send(&n_slave, 1, MPI_LONG, i, 0, MPI_COMM_WORLD); 
      }
      free(fileName);
      fclose(tempFile); 

      delete []array_slave;
      delete []array_master;

      close(fin);
    } 
    else{
      MPI_Status status;
      long n=0;
      long *array = new long[m_count];
      memset(array, 0, m_count);

     
      MPI_Recv(&n, 1, MPI_LONG, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status );
//      printf("counter: %d, rank:%d, size: %li, m_count: %li\n",counter, myRank, n, m_count);
      while(n!=0){
        counter++;
        MPI_Recv(array, n, MPI_LONG, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
//        printf("counter: %d, rank:%d\n",counter, myRank);
        mpi_sort(array,n);
        MPI_Recv(&n, 1, MPI_LONG, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status );
      } 
    }
//    printf("rank: %d, counter:%d\n",myRank, counter);

    return file_count;
  }

  void merge_sort(long file_count)
  {
    if(file_count <= 0) return;
    FILE *fout = fopen(m_out_file, "wt");
    FILE* *farray = new FILE*[file_count];
    long i;
    unsigned long num_cnt = 0;

    for(i = 0; i < file_count; ++i)
    {
      char* fileName = temp_filename(i);
      farray[i] = fopen(fileName, "rt");
      free(fileName);
    }

    int *data = new int[file_count];
    bool *hasNext = new bool[file_count];
    memset(data, 0, sizeof(int) * file_count);
    memset(hasNext, 1, sizeof(bool) * file_count);

    for(i = 0; i < file_count; ++i)
    {
      if(fscanf(farray[i], "%d", &data[i]) == EOF)
        hasNext[i] = false;
    }
    while(true)
    {
      int max = data[0];
      int j = 0;
      for(i = 0; i < file_count; ++i)
      {
        if(hasNext[i] && max > data[i])
        {
          max = data[i];
          j = i;
        }
      }
      if(j == 0 && !hasNext[0]) break; 

      if(fscanf(farray[j], "%d", &data[j]) == EOF) 
        hasNext[j] = false;
      if(num_cnt%10==0)
        fprintf(fout, "\n");

      fprintf(fout, "%d ", max);
      num_cnt++;
    }

    delete [] hasNext;
    delete [] data;

    for(i = 0; i < file_count; ++i)
    {
      fclose(farray[i]);
    }
    delete [] farray;
    fclose(fout);
  }
};

#endif
