#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>

#define MASTER 0

float distance(float *arr1, float *arr2, int size) {
	int i;
	float result = 0;

	for (i=0; i<size; i++) {
		result += pow(arr2[i] - arr1[i], 2);
	}

	return result;
}


void loadline(char* line, int dim, float *arr)
{
    const char* tok;
    int i = 0;
    
    for (tok = strtok(line, ",");
            tok && *tok;
            tok = strtok(NULL, ",\n"))
    {
	if (i < dim) 
	{
        	arr[i++] = atof(tok);
	}
    }
}

int main(int argc, char** argv) {
	
	MPI_Status status;
	MPI_Request request;
	char name[BUFSIZ];
    int my_id, ierr, num_procs, seed, name_length, an_id;
    int i, j, p, t, k, data_size, dimension, max_range, iteration;
    float mse, old_mse, local_mse;
    double start_time, end_time, start_comm_time, end_comm_time, total_comm_time, end_init_time;
    char* filename;

    ierr = MPI_Init(&argc, &argv);
 
    // Get the number of processes
	MPI_Comm_size(MPI_COMM_WORLD, &num_procs);

	// Get the rank of the process
	MPI_Comm_rank(MPI_COMM_WORLD, &my_id);

	// Get the name of the processor
	MPI_Get_processor_name(name, &name_length);

	printf("Running process on %s (%d/%d)\n", name, my_id, num_procs);

	// Arguments
	if (argc == 6) {
		// arg0 is the executable
		k = atoi(argv[1]);
		data_size = atoi(argv[2]);
		dimension = atoi(argv[3]);
		max_range = atoi(argv[4]);
		filename = argv[5];
		seed = 10;
		srandom(seed);
		total_comm_time = 0;

		//printf("KMeans k%d s%d d%d r%d f%s\n", k, data_size, dimension, max_range, filename);
	} else {
		printf("KMeans Usage: <Centers> <Data_size> <Dimension> <MaxRange> <FilenamePrefix>\n");
		exit(0);
	}

	// Open file stream
	FILE* stream = fopen(filename, "r");
    
    // Set the data size for each partition
    data_size = data_size / num_procs;
    
    // Allocate memory for local vectors
	float **data;
    data = malloc(sizeof(float *) * data_size);

    for (i=0; i<data_size; i++) {
    	data[i] = malloc(sizeof(float) * dimension);
    }

    // Read file and send partitions
    if (my_id == MASTER) {
    	float *temp_arr;
    	temp_arr = malloc(sizeof(float) * dimension);

	    // Read file and create vectors
	    char line[1024];
	    int line_number = 0;
	    while (fgets(line, 1024, stream) && line_number < (data_size * num_procs)) {
	    	char* tmp = strdup(line);
	        loadline(tmp, dimension, temp_arr);
	        
	        if (line_number < data_size) {
	        	for (i=0; i<dimension; i++) {
	        		data[line_number][i] = temp_arr[i];
	        	}
	        } else {
	        	an_id = line_number/data_size;
	        	MPI_Send(temp_arr, dimension, MPI_FLOAT, an_id, 3001, MPI_COMM_WORLD);
	        }
	        
	        line_number++;
	    }
	} else {
		int line_number = 0;

		for (i=0; i<data_size; i++) {
			MPI_Recv(data[line_number++], dimension, MPI_FLOAT, MASTER, 3001, MPI_COMM_WORLD, &status);
		}
	}	

    MPI_Barrier(MPI_COMM_WORLD);

    // Start timing
    start_time = MPI_Wtime();
    
    // Create initial centers (Random)
	float centers[k][dimension];

	if (my_id == MASTER) {
		for (i=0; i<k; i++) {
			for (j=0; j<dimension; j++) {
				centers[i][j] = random() % max_range;
			}
		}
	}
	MPI_Bcast(centers, k * dimension, MPI_INT, MASTER, MPI_COMM_WORLD);

	// End initialization phase
	end_init_time = MPI_Wtime();
	
	/*if (my_id == MASTER) {
		printf("Initial Centers\n");
		for (i=0; i<k; i++) {
			printf("\t%d %d(%f, %f)\n", my_id, i, centers[i][0], centers[i][1]);
		}
	}*/
	
	// n keeps the count for the new center
	int n[k];
	int local_n[k];

	// keeps the sum for the new center
	float local_sum[k][dimension];

	mse = -1;
	iteration = 1;
	
	

	do {
		MPI_Barrier(MPI_COMM_WORLD);

		/*if (my_id == MASTER) {
			printf("---- ITERATION %d ----\n", iteration);
		}*/

		iteration++;
		old_mse = mse;
		local_mse = 0;


		// Initialize data for new centers
		for (i=0; i<k; i++) {
			local_n[i] = 0;
			
			for (j=0; j<dimension; j++) {
				local_sum[i][j] = 0;
			}
		}

		// Calculate distance 
		for (i=0; i<data_size; i++) {
			int min_center = -1;
			float min_distance = -1;

			for (j=0; j<k; j++) {
				float temp_distance = distance(data[i], centers[j], dimension);

				if (min_center == -1 || temp_distance < min_distance) {
					min_center = j;
					min_distance = temp_distance;
				}
			}

			// with the assigned cluster center, update sum and count
			for (j=0; j<dimension; j++) {
				local_sum[min_center][j] += data[i][j];
			}
			local_n[min_center] ++;

			// add square distance to MSE
			local_mse += pow(min_distance, 2);
		}

		// START: Centroid Recalculation Phase (communication)
		start_comm_time = MPI_Wtime();
		
		// Aggregate centers information
		for (j=0; j<k; j++) {
			MPI_Allreduce(&local_sum[j], &centers[j], dimension, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);
			MPI_Allreduce(&local_n[j], &n[j], 1, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);
		}

		// Aggregate MSE
		MPI_Allreduce(&local_mse, &mse, 1, MPI_FLOAT, MPI_SUM, MPI_COMM_WORLD);

		for (j=0; j<k; j++) {
			if (n[j] == 0) {
				n[j] = 1;
			}

			for (t=0; t<dimension; t++) {
				centers[j][t] = centers[j][t] / n[j];
			}
		}
		
		end_comm_time = MPI_Wtime();
		total_comm_time += end_comm_time - start_comm_time;
		// END: Centroid Recalculation Phase (communication)


		/*if (my_id == MASTER) {
			printf("\tMSE old = %f  ::: new = %f\n", old_mse, mse);

			printf("\tCenters:\n");
			for (i=0; i<k; i++) {
				printf("\t\t(%f, %f)\n", centers[i][0], centers[i][1]);
			}
		}*/

	} while (mse < old_mse || old_mse == -1);
	// while (iteration < 10);

	MPI_Barrier(MPI_COMM_WORLD);

	end_time = MPI_Wtime();

	if (my_id == MASTER) {
		printf("TIME %f s.\n", (end_time-start_time));
		printf("Communication Time %f s.\n", total_comm_time);
		printf("Initialization Time %f s.\n", (end_init_time - start_time));
		printf("Iterations: %d\n", (iteration - 1));
	}

	ierr = MPI_Finalize();
}

// mpicc kmeans.c -lm -O -o kmeans
// mpirun -np 2 ./kmeans 8 8192 8 300 dataset.txt
