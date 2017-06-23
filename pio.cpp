/*
 * pio.cpp
 *
 *  Created on: Jun 22, 2017
 *      Author: weiz
 */

#include <mpi.h>
 #include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <iostream>
#include <assert.h>
using namespace std;


void work(char *src, char *dst, int newName, int chunkSz){
	int rank, worldSize;
	MPI_Init(NULL, NULL);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &worldSize);
	struct timeval start, end;
	int mpiret = 0;
	size_t sz = 0;
	if(rank == 0){ /*sender */
		// step 1 figure out how many rounds and the last offset
		struct stat st;
		stat(src, &st);
		long srcSz  = st.st_size;
		long rounds = srcSz / (long)chunkSz;
		long offset = srcSz % (long ) chunkSz;
		long meta[2]={rounds, offset};

		mpiret = MPI_Bcast(meta, 2, MPI_LONG, 0, MPI_COMM_WORLD);
		assert(mpiret == MPI_SUCCESS);
		// step 2 bcast data
		FILE *srcFile = fopen(src, "rb");
		assert(srcFile != NULL);
		unsigned char *readBuf = (unsigned char*)malloc(chunkSz);
		for(long i = 0; i < rounds; ++i){
			gettimeofday(&start, NULL);
			 sz = fread(readBuf, 1, chunkSz, srcFile);
			assert(sz == chunkSz);
			mpiret = MPI_Bcast(readBuf, chunkSz, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
			assert(mpiret == MPI_SUCCESS);
			gettimeofday(&end, NULL);
			printf("Iter %ld/%ld takes %f sec\n", i, rounds,(float)((end.tv_sec * 1000000 + end.tv_usec)
					  - (start.tv_sec * 1000000 + start.tv_usec))/(float)100000);
		}
		sz = fread(readBuf, 1, offset, srcFile);
		assert(sz == offset);
		mpiret = MPI_Bcast(readBuf, offset, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
		assert(mpiret == MPI_SUCCESS);
		assert(fclose(srcFile) == 0);
		free(readBuf);
	}else{/*reciever */
		//step 1 get meta-data
		long meta[2];
		MPI_Bcast(meta, 2, MPI_LONG, 0, MPI_COMM_WORLD);
		long rounds = meta[0];
		long offset = meta[1];
		std::string str;
		if(newName != 0){
			str = std::string(dst)+".r"+std::to_string(rank);
		}else{
			str = std::string(dst);
		}
		FILE *destFile = fopen(str.c_str(), "wb");
		assert(destFile != NULL);
		unsigned char *writeBuf = (unsigned char*)malloc(chunkSz);
		for(long long i = 0;  i < rounds; ++i){
			mpiret = MPI_Bcast(writeBuf, chunkSz, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
			assert(mpiret ==  MPI_SUCCESS);
			sz = fwrite(writeBuf, 1, chunkSz, destFile);
			assert(sz == chunkSz);
		 }
		mpiret = MPI_Bcast(writeBuf, offset, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD);
		assert(mpiret == MPI_SUCCESS);
		sz = fwrite(writeBuf, 1, offset, destFile);
		assert(sz == offset);
		assert(fclose(destFile) == 0);
		free(writeBuf);
	}

	MPI_Finalize();
}

void parse(int argc, char **argv){
	int c;
	int nflag = 0;
	char *sval = NULL;
	char *dval = NULL;
	char *cval = NULL;
	int chunkSize = 1e6; // 4MB chunksize
	int index;

	while ((c = getopt (argc, argv, "s:d:c:nh")) != -1){
		switch (c)
		{
		case 's':
	        sval = optarg;
	        break;
	      case 'd':
	        dval = optarg;
	        break;
	      case 'c':
	        cval = optarg;
	        break;
	      case 'n':
	    	  	  nflag = 1;
	    	  	  break;
	      case 'h':
	    	  	std::cout<<"Usage: "<<argv[0]<<" -s src -d dst -c chunksize [-n newname]"<<std::endl;
	    	  	exit(0);
	      default:
	        abort ();
		}
	}
	printf("s: %s, d: %s, chunk: %d, nflag: %d\n", sval, dval, atoi(cval), nflag);
	work(sval, dval, nflag, atoi(cval));
}

int main(int argc , char **argv){
	parse(argc, argv);
}

