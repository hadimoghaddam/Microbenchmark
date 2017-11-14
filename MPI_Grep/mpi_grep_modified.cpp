#define _LARGEFILE_SOURCE
#define _LARGEFILE64_SOURCE
#define _FILE_OFFSET_BITS 64


#include "mpi.h"
#include <stdio.h>      /* printf, fopen */
#include <stdlib.h>     /* exit, EXIT_FAILURE */
#include <string.h>

#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>

#include <iostream>
#include <string>



#define BUFSIZE 1024
#define MAX_PATH 1024

int is_match(const std::string& text, const std::string& pattern)
{
  // FIXME TODO use actual (posix?) regex, boost regex, regex++ or whatnot
  size_t pos = text.find(pattern, 0);
  int sub_found_cnt = 0;
  while(pos != std::string::npos)
  {
    pos = text.find(pattern,pos+1);
    sub_found_cnt++;
  }
  return sub_found_cnt;
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


int processFile(const char *fileName, unsigned long &match_count, int &rank, int &size){
  int from_fd;
  char line[BUFSIZE];
  int size_mpi=0;
  int bytes_read = 1; //start
  if ((from_fd = open(fileName, O_RDONLY|O_LARGEFILE)) == -1) {
    fprintf(stderr, "Open %s Error %s\n", fileName, strerror(errno));
    exit(1);
  }
  
  while(bytes_read > 0){
    for(int i=1; i<size; i++){
      if (bytes_read = read(from_fd, line, BUFSIZE-1) ) {
        if ((bytes_read == -1) && (errno != EINTR)) break;
        else if (bytes_read > 0) {
          if (line[strlen(line) - 1] == '\n')
            line[strlen(line)-1] = '\0';
          if (line!=NULL)
          {
            size_mpi = strlen(line) + 1;
            MPI_Send(&size_mpi, 1, MPI_INT, i, 0, MPI_COMM_WORLD); 
            MPI_Send(line, size_mpi, MPI_CHAR, i, 0, MPI_COMM_WORLD);
          }
        }
      }
    }
  }

  size_mpi = 0;
  for(int i=1 ;i<size; i++){
    MPI_Send(&size_mpi, 1, MPI_INT, i, 0, MPI_COMM_WORLD); 
  }

  close(from_fd);
  
  return 1;
}


int main(int argc, char* argv[])
{
  switch(argc)
  {
    case 0: case 1: case 2:
      std::cerr << "Useage: ./mpi_grep [input_file] [pattern]" << std::endl;
      return 254;
    case 3:
      break;
    default:
      std::cerr << "Useage: ./mpi_grep [input_file] [pattern]" << std::endl;
      return 255;
  }
  int rank, size, alpha_index_begin, alpha_index_end;
  char   processor_name[MPI_MAX_PROCESSOR_NAME];
  int    namelen;
  double startwtime = 0.0, endwtime;
  MPI_Status mpi_status;

  MPI_Init(&argc,&argv); /* starts MPI */
  MPI_Comm_rank(MPI_COMM_WORLD, &rank); /* get current process id */ 
  MPI_Comm_size(MPI_COMM_WORLD, &size); /* get number of processes */

  MPI_Get_processor_name(processor_name,&namelen);

  fprintf(stdout,"Process %d of %d is on %s\n",rank, size, processor_name);
  fflush(stdout);

  if (rank == 0)
    startwtime = MPI_Wtime();

  unsigned long match_count = 0;
  
  char* fileName = argv[1]; 
  struct stat buf; 
  int result;
  int counter = 0;
  MPI_Status status; 

  result = stat( fileName, &buf ); 
  
  if(rank==0){
  
    if(S_IFDIR & buf.st_mode){ 

      printf("##folder\n"); 
  
      char * pFilePath = argv[1];
      DIR * dir;
      struct dirent * ptr;
      struct stat stStatBuf;
  
      dir = opendir(pFilePath);
      if(dir == NULL){
        printf("ERROR: Can't open dir \"%s\"\n",pFilePath);
        return 0;
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
            processFile( Path,match_count,rank,size);
  
          }
        }
        closedir(dir);
      }
  
    }else if(S_IFREG & buf.st_mode){ 
      printf("###file\n");
      processFile( argv[1],match_count,rank,size);
    } 
  }

  else{
    char line[BUFSIZE];
    int size_mpi = 0;
    memset(line, 0, BUFSIZE);
    
    MPI_Recv(&size_mpi, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status );
    while(size_mpi!=0){
      counter++;
      MPI_Recv(line, size_mpi, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
      std::string str(line);
      std::string pattern(argv[2]);
      is_match(line, pattern);
      MPI_Recv(&size_mpi, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status );
    } 
  }

  printf("rank: %d, counter:%d\n",rank, counter);
 
  unsigned long final_total_cnt;

  MPI_Reduce(&match_count, &final_total_cnt, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD); 

  if (rank == 0){
    std::cout<<"Total Count "<<final_total_cnt<<std::endl;

    endwtime = MPI_Wtime();
    std::cout<<"wall clock time = "<<endwtime-startwtime<<std::endl;
  }  

  MPI_Finalize(); 

  return 0;

}
