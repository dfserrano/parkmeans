#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <sys/time.h>

float distance(float *arr1, float *arr2, int size) {
	int i;
	float result = 0;

	for (i=0; i<size; i++) {
		result += pow(arr2[i] - arr1[i], 2);
	}

	return result;
}


void loadline(char* line, int line_num, float **arr)
{
    const char* tok;
    int i = 0;
    for (tok = strtok(line, ",");
            tok && *tok;
            tok = strtok(NULL, ",\n"))
    {
        arr[line_num][i++] = atof(tok);
    }
}

int main(int argc, char** argv) {
	
	int seed;
    int i, j, p, t, k, data_size, dimension, max_range;
    float mse, old_mse;
    struct timeval start_time, end_time;
    char* filename;

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
	} else {
		printf("KMeans Usage: <Centers> <Data_size> <Dimension> <MaxRange> <FilenamePrefix>\n");
		exit(0);
	}

	char my_id_string[5];
	sprintf(my_id_string, ".txt");
	strcat(filename, my_id_string);
	FILE* stream = fopen(filename, "r");
    char line[1024];

	mse = -1;

	float **data;
    data = malloc(sizeof(float *) * data_size);

    for (i=0; i<data_size; i++) {
    	data[i] = malloc(sizeof(float) * dimension);
    }

    int line_number = 0;
    while (fgets(line, 1024, stream) && line_number < data_size) {
    	char* tmp = strdup(line);
        loadline(tmp, line_number, data);
        
        line_number++;
    }

    gettimeofday(&start_time, NULL);

	float cur_sum_centers[k][dimension];
	float centers[k][dimension];

	//printf("Creating centers\n");
	for (i=0; i<k; i++) {
		for (j=0; j<dimension; j++) {
			centers[i][j] = random() % max_range;
		}
	}

	/*printf("Initial Centers\n");
	for (i=0; i<k; i++) {
		printf("\t(%f, %f)\n", centers[i][0], centers[i][1]);
	}*/
	
	int n[k];
	float m[k];
	int iteration = 1;

	do {
		//printf("---- ITERATION %d ----\n", iteration);
		
		iteration++;
		old_mse = mse;
		mse = 0;

		// Initialization
		for (i=0; i<k; i++) {
			n[i] = 0;
			
			for (j=0; j<dimension; j++) {
				cur_sum_centers[i][j] = 0;
			}
		}

		// Calculating distances
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

			for (j=0; j<dimension; j++) {
				cur_sum_centers[min_center][j] += data[i][j];
			}
			n[min_center] ++;

			mse += pow(min_distance, 2);
		}

		// Calculating new centers
		for (j=0; j<k; j++) {
			if (n[j] == 0) {
				n[j] = 1;
			}

			for (t=0; t<dimension; t++) {
				centers[j][t] = cur_sum_centers[j][t] / n[j];
			}
		}

		/*printf("\tMSE old = %f  ::: new = %f\n", old_mse, mse);

		printf("\tCenters:\n");
		for (i=0; i<k; i++) {
			printf("\t\t(%f, %f)\n", centers[i][0], centers[i][1]);
		}*/

	} while (mse < old_mse || old_mse == -1);
	// while (iteration < 10);

	gettimeofday(&end_time, NULL);

	long total_time = (end_time.tv_sec * 1000000 + end_time.tv_usec)
		  - (start_time.tv_sec * 1000000 + start_time.tv_usec);

	printf("Iterations: %d\n", (iteration - 1));
	printf("Time: %ld\n", (total_time / 1000));
}
// gcc seq.c -lm -O -o seq
// ./seq 8 8192 8 300 dataset.txt