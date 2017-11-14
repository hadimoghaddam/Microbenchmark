#include <iostream>
#include <ctime>
#include <fstream>
#include <time.h>
#include "ExternSort_mod.h"

using namespace std;

#define DEBUG

const unsigned int number_to_sort = 50000000;
void init_random_data(unsigned int num); 

int main(int argc, char* *argv)
{
  switch(argc)
  {
    case 0: case 1: case 2:
      std::cerr << "Useage: ./mpi_sort [input_file] [output_file]" << std::endl;
      return 254;
    case 3:
      break;
    default:
      std::cerr << "Useage: ./mpi_sort [input_file] [output_file]" << std::endl;
      return 255;
  }
  //srand(time(NULL));
  //  init_random_data(data_count);
  time_t start = time(NULL);
  time_t timep;
  time (&timep);
  printf("%s",ctime(&timep));
  int myRank, nProc;
  int rc;
  int   size;          // Size of the vector being sorted
  //double start,        // Begin parallel sort
  //     middle,       // Finish parallel sort
  //     finish;       // Finish sequential sort

  rc = MPI_Init(&argc, &argv);
  //  srand(time(NULL));// Set up for shuffling

  if ( rc < 0 )
  {
    puts ("Failed to enroll in MPI.  Abort!");
    exit(-1);
  }

  //  if ( argc > 1 )
  //    size = atoi(argv[1]);
  rc = MPI_Comm_rank (MPI_COMM_WORLD, &myRank);
  rc = MPI_Comm_size (MPI_COMM_WORLD, &nProc);

#ifdef DEBUG
  printf ("Started rank %d\n", myRank);  fflush(stdout);
#endif

#ifdef DEBUG
  printf ("Input File:%s, Output File:%s \n", argv[1], argv[2]);  fflush(stdout);
#endif
  ExternSort extSort(argv[1], argv[2], number_to_sort);
  extSort.sort();

  if(myRank==0)
  {
    time_t end = time(NULL);
  //  printf("total time:%f\n", (end - start) * 1000.0/ CLOCKS_PER_SEC);
    printf("Total running time:%f sec\n", (end - start)*1.0 );
    //  printf ("  Parallel:  %3.3f\n", (middle-start) );
    //  printf ("Sequential:  %3.3f\n", (finish-middle) );
    //  printf ("  Speed-up:  %3.3f\n", (finish-middle)/(middle-start) );
    time_t time_end;
    time (&time_end);
    printf("%s",ctime(&time_end));
    system("rm temp*.txt");
  }
  MPI_Finalize();
  return 0;
}

