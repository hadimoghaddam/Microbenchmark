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

#include <map>
#include <iostream>


#define BUFSIZE 1024
#define MAX_PATH 1024


unsigned long max_cnt;
unsigned long total_cnt;
unsigned long diff_cnt;
const char *max_str = NULL;
struct StrLess
{
  bool operator() (const char* s1, const char* s2) const 
  { 
    return strcmp(s1, s2) < 0;
  } 
};

typedef std::map<const char*, int, StrLess> word_map_t;

static void parseAndCollect(char line[], word_map_t& wmap)
{
  int  alpha_index_begin=0, alpha_index_end=25;
  int idx = 0;
  char word_buf[124];
  char lead_char_range_begin = 'a' + alpha_index_begin; 
  char lead_char_range_end = 'a' + alpha_index_end;
  while (line[idx] != '\0') 
  {
    while ( isalnum(line[idx]) == 0) 
    { // skip non alnum chars
      idx++; 
      if (line[idx] == '\0') 
        return;
    }
    int cnt = 0;
    while( isalnum(line[idx]) != 0) 
    {
      if (line[idx] == '\0') break;
      // convert the word to lower case
      word_buf[cnt] = ((isupper(line[idx])!=0) ? tolower(line[idx]):line[idx]); 
      idx++; cnt++;
    }
    word_buf[cnt] = '\0';
    if (word_buf[0] >= lead_char_range_begin && word_buf[0] <= lead_char_range_end){
      char* new_word = new char[strlen(word_buf) + 1]; 
      strcpy(new_word, word_buf);
      char* cur_word = new_word;
      word_map_t::iterator hit = wmap.find(cur_word); total_cnt++;
      if (hit != wmap.end()) 
      {
        (*hit).second = (*hit).second + 1; // increase the count.
        if ((*hit).second > max_cnt) 
      	{
      	  max_cnt=(*hit).second; max_str =(*hit).first; 
      	} 
      }
      else 
      {
        word_map_t::value_type new_item(cur_word, 1);
        wmap.insert(new_item );
        if (max_cnt< 1) 
        { 
      	  max_cnt = 1; max_str = cur_word;
        }
        diff_cnt++;
      }
      delete new_word;
    }
  }
}

int processFile(const char *fileName,  int size){
  int from_fd;
  int bytes_read=1; //start
  char line[BUFSIZE];
  unsigned long line_count = 0;
  int size_mpi =0;
  if ((from_fd = open(fileName, O_RDONLY|O_LARGEFILE)) == -1) {
    fprintf(stderr, "Open %s Error %s\n", fileName, strerror(errno));
    exit(1);
  }
  while(bytes_read>0){
    for(int i=1 ;i<size; i++){
      if(bytes_read = read(from_fd, line, BUFSIZE-1)) {
        if ((bytes_read == -1) && (errno != EINTR)) break;
        else if (bytes_read > 0) {
          if (line[strlen(line) - 1] == '\n')
            line[strlen(line)-1] = '\0';
        char* new_line = new char[strlen(line) + 1];
        strcpy(new_line, line);
        size_mpi = strlen(line) + 1;
        MPI_Send(&size_mpi, 1, MPI_INT, i, 0, MPI_COMM_WORLD); 
        MPI_Send(new_line, size_mpi, MPI_CHAR, i, 0, MPI_COMM_WORLD); 
  
        delete new_line;
        line_count++;
        
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
  int counter=0; 
 
  switch(argc)
  {
  case 0: case 1:
  	std::cerr << "Useage: ./mpi_wordcount [input_file]" << std::endl;
  	return 254;
  case 2:
  	break;
  default:
  	std::cerr << "Useage: ./mpi_wordcount [input_file]" << std::endl;
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
  
  std::cout << "Rank:" << rank << " ##"<<std::endl;
  std::cout << "Size:" << size << " ##"<<std::endl;
  MPI_Get_processor_name(processor_name,&namelen);
  
  fprintf(stdout,"Process %d of %d is on %s\n",
  		rank, size, processor_name);
  fflush(stdout);
  
  max_cnt = 0; total_cnt = 0; diff_cnt = 0;
  
  int portion = 26 / size; 
  if (26 % size) 
  	portion++;
  
  alpha_index_begin = rank* portion;
  alpha_index_end = alpha_index_begin + (portion -1); 
  if (alpha_index_end > 25) alpha_index_end = 25;
  if (rank == 0)
  startwtime = MPI_Wtime();
  word_map_t wmap;
  char* fileName = argv[1]; 
  struct stat buf; 
  int result;
  MPI_Status status; 

  result = stat( fileName, &buf ); 
  
  if(rank==0){
    if(S_IFDIR & buf.st_mode){ 
  
      printf("##folder\n"); 
      
      char * pFilePath = argv[1];
      DIR * dir;
      struct dirent * ptr;
      struct stat stStatBuf;
      
      //	chdir(pFilePath);
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
    	  //processFile( Path,wmap,alpha_index_begin, alpha_index_end);
    	  processFile( Path, size);
      	}
        }
        closedir(dir);
      }
  
    }else if(S_IFREG & buf.st_mode){ 
      printf("###file\n");
      processFile( argv[1], size);
    }    
  }
  //worker threads
  else{
    char line[BUFSIZE];
    int size_mpi = 0;
    memset(line, 0, BUFSIZE);
    
    MPI_Recv(&size_mpi, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status );
    while(size_mpi!=0){
      counter++;
      MPI_Recv(line, size_mpi, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
      parseAndCollect(line, wmap);
      MPI_Recv(&size_mpi, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status );

    } 
  
  }
  printf("rank: %d, counter:%d\n",rank, counter);
  
  unsigned long final_total_cnt, final_diff_cnt, final_max_cnt;
  MPI_Reduce(&diff_cnt, &final_diff_cnt, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD); 
  MPI_Reduce(&total_cnt, &final_total_cnt, 1, MPI_LONG_LONG_INT, MPI_SUM, 0, MPI_COMM_WORLD); 
  MPI_Reduce(&max_cnt, &final_max_cnt, 1, MPI_LONG_LONG_INT, MPI_MAX, 0, MPI_COMM_WORLD);
  if (rank == 0){
    std::cout<<"Total words "<<final_total_cnt<<" Diff cnt "<<final_diff_cnt<<" Max Cnt "<<final_max_cnt<<std::endl;
    endwtime = MPI_Wtime();
    std::cout<<"wall clock time = "<<endwtime-startwtime<<std::endl;
  }  

  MPI_Finalize(); 
  return 0; 
}
