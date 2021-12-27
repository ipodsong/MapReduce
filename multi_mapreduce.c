#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include <pthread.h>
#include "mapreduce.h"

char temp[1000];
//characters for file name

int nr_partition;
int *count_partition;

//pthread_mutex_t sync_mutex;
//pthread_cond_t  sync_cond;

//pthread_mutex_t gmutex = PTHREAD_MUTEX_INITIALIZER;
//pthread_mutex_t kmutex = PTHREAD_MUTEX_INITIALIZER;
//pthread_cond_t  gcond  = PTHREAD_COND_INITIALIZER;

//data structure
struct MAP{
	int ref;
	char Key[50];
	char Value[5];
	struct MAP *next_node;
};
typedef struct MAP MAP;
//struct MAP has data and pointer to next node

struct MultipleArg{
	char key[50];
	Getter get;
	int num;
};
typedef struct MultipleArg MultipleArg;

//initializing structure


void MAP_add(MAP** root, char* key, char* value){
	//pthread_mutex_lock(&kmutex);
	MAP *new_MAP = (MAP*)malloc(sizeof(MAP));
	new_MAP->ref = 0;
	strcpy(new_MAP->Key, key);
	strcpy(new_MAP->Value, value);
	new_MAP->next_node = NULL;
	
	if(root == NULL){
		*root = new_MAP;
	}
	else{
		new_MAP->next_node = *root;
		*root = new_MAP;
	}
	//pthread_mutex_unlock(&kmutex);
	return;
}
//add node

MAP *tempMap;
MAP **shuffleMap;

Mapper Mappertemp;
Reducer Reducertemp;
Partitioner partitiontemp;

char* Put_next(char *key, int partition_number){
	//pthread_mutex_lock(&kmutex);
	char *temp = NULL;

	while(((shuffleMap[partition_number]->ref)==1)&&((shuffleMap[partition_number]->next_node) != NULL)){
		shuffleMap[partition_number] = shuffleMap[partition_number]->next_node;
	}

	if((shuffleMap[partition_number]->ref) == 0){
		shuffleMap[partition_number]->ref = 1;
		return shuffleMap[partition_number]->Key;
	}
	else if(shuffleMap[partition_number] == NULL){
		return temp;
	}
	//pthread_mutex_unlock(&kmutex);
	return temp;
}

void *t_function(void *data){
	char *temp;
	temp = (char*)data;
	//pthread_mutex_lock(&gmutex);
	Mappertemp(temp);
	//pthread_mutex_unlock(&gmutex);

	//pthread_mutex_lock(&sync_mutex);
	//pthread_cond_signal(&sync_cond);
	//pthread_mutex_unlock(&sync_mutex);
	return NULL;
}

void *t_function2(void *data){
	MultipleArg *my_multiple_arg = (MultipleArg*)malloc(sizeof(MultipleArg));
	//memcpy(my_multiple_arg, data, sizeof(MultipleArg));
	my_multiple_arg = (MultipleArg*)data;
	//pthread_mutex_lock(&gmutex);
	Reducertemp(my_multiple_arg->key, my_multiple_arg->get, my_multiple_arg->num);
	//pthread_mutex_unlock(&gmutex);

	//pthread_mutex_lock(&sync_mutex);
	//pthread_cond_signal(&sync_cond);
	//pthread_mutex_unlock(&sync_mutex);
	return NULL;
}


void MR_Emit(char *key, char *value){
	//pthread_mutex_lock(&gmutex);
	int i = 1;
	if(shuffleMap[partitiontemp(key, nr_partition)] == NULL){
		MAP_add(&shuffleMap[partitiontemp(key, nr_partition)], key, value);
		count_partition[partitiontemp(key, nr_partition)] = 1;
	}
	else if(strcmp(shuffleMap[partitiontemp(key, nr_partition)]->Key, key) == 0){
		MAP_add(&shuffleMap[partitiontemp(key, nr_partition)], key, value);
		count_partition[partitiontemp(key, nr_partition)] = 1;
	}
	else{
		while(1){
			if(shuffleMap[(partitiontemp(key, nr_partition)+i)%nr_partition] == NULL)	break;
			else if(strcmp(shuffleMap[(partitiontemp(key, nr_partition)+i)%nr_partition]->Key, key) == 0)	break;
			i++;
			if(i>nr_partition){
				printf("no space %d %s\n",i, key);
				break;
			}
		}
		MAP_add(&shuffleMap[(partitiontemp(key, nr_partition)+i)%nr_partition], key, value);
		count_partition[(partitiontemp(key, nr_partition)+i)%nr_partition] = 1;
	}
        //pthread_mutex_unlock(&gmutex);

}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition){
	int input_simple = 0, input_long_balanced = 0, input_long_unbalanced = 0;
	//int status = 0;
	//int count_for_map = 0;
	//int count_for_reduce = 0;
	//init variable
	Mappertemp = map;
	Reducertemp = reduce;
	partitiontemp = partition;
	//functions
	//pthread_t map_thread[num_mappers];
	//pthread_t reduce_thread[num_reducers];
	//declare threads
	

	if(!strcmp(argv[1],"input_simple/")){
		input_simple = 1;
		nr_partition = atoi(argv[2]);
	}
	else if(!strcmp(argv[1],"input_long_balanced/")){
		input_long_balanced = 1;
		nr_partition = atoi(argv[2]);
	}
	else if(!strcmp(argv[1],"input_long_unbalanced/")){
		input_long_unbalanced = 1;
		nr_partition = atoi(argv[2]);
	}
	//get information from arguments

	count_partition = (int*)malloc(sizeof(int)*nr_partition);
	int i;
	int j;
	for(i = 0; i < nr_partition; i++){
		count_partition[i] = 0;
	}

	shuffleMap=(MAP**)malloc(sizeof(MAP)*nr_partition);
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	if(input_simple == 1){
		DIR* dp = NULL;
		struct dirent* entry = NULL;
		
		if((dp = opendir("./input_simple")) == NULL){
			printf("Failed to open file\n");
		}//open directory

		//pthread_mutex_init(&sync_mutex, NULL);
		//pthread_cond_init(&sync_cond, NULL);

		while(((entry = readdir(dp)) != NULL)){
			if(strcmp(entry->d_name, ".")&&strcmp(entry->d_name, "..")){
				strcpy(temp, "./input_simple/");
				strcat(temp,entry->d_name);
				map(temp);
				/*pthread_mutex_lock(&sync_mutex);
				if(pthread_create(&map_thread[count_for_map], NULL, t_function, (void*)&temp)<0){
					printf("error\n");
				}
				pthread_cond_wait(&sync_cond, &sync_mutex);
				pthread_mutex_unlock(&sync_mutex);
				pthread_join(map_thread[count_for_map], (void**)&status);
				count_for_map++;
				if(count_for_map == num_mappers){
					count_for_map = 0;
				}*/
			}
		}
		//Read all files in the folder and call map function
		closedir(dp);


		for(i=0;i<nr_partition;i++){
			for(j=i;j<nr_partition;j++){
				if((shuffleMap[i]!=NULL)&&(shuffleMap[j]!=NULL)&&(strcmp(shuffleMap[i]->Key, shuffleMap[j]->Key)>0)){
					MAP **temp = (MAP**)malloc(sizeof(MAP)*2);
					temp[0] = shuffleMap[i];
					temp[1] = shuffleMap[j];
					shuffleMap[j] = temp[0];
					shuffleMap[i] = temp[1];
				}
			}
		}
		//sorting

		//MultipleArg *multiple_arg;
		//multiple_arg = (MultipleArg*)malloc(sizeof(MultipleArg));

		for(i = 0 ; i < nr_partition ; i++){
			if(count_partition[i] == 1){
				reduce(shuffleMap[i]->Key, Put_next, i);
				/*multiple_arg->get = &Put_next; 
				strcpy(multiple_arg->key, shuffleMap[i]->Key);
				multiple_arg->num = i;
				
				pthread_mutex_lock(&sync_mutex);
				if(pthread_create(&reduce_thread[count_for_reduce], NULL, t_function2, multiple_arg)<0){
					printf("error\n");
				}
				pthread_cond_wait(&sync_cond, &sync_mutex);
				pthread_mutex_unlock(&sync_mutex);

				pthread_join(reduce_thread[count_for_reduce], (void**)&status);
				count_for_reduce++;
				if(count_for_reduce == num_reducers){
					count_for_reduce = 0;
				}*/
			}
		}
		//reduce
	}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	if(input_long_balanced == 1){
		DIR* dp = NULL;
		struct dirent* entry = NULL;
		
		if((dp = opendir("./input_long_balanced")) == NULL){
			printf("Failed to open file\n");
		}//open directory

		//pthread_mutex_init(&sync_mutex, NULL);
		//pthread_cond_init(&sync_cond, NULL);

		while(((entry = readdir(dp)) != NULL)){
			if(strcmp(entry->d_name, ".")&&strcmp(entry->d_name, "..")){
				strcpy(temp, "./input_long_balanced/");
				strcat(temp,entry->d_name);
				map(temp);
				/*pthread_mutex_lock(&sync_mutex);
				if(pthread_create(&map_thread[count_for_map], NULL, t_function, (void*)&temp)<0){
					printf("error\n");
				}
				pthread_cond_wait(&sync_cond, &sync_mutex);
				pthread_mutex_unlock(&sync_mutex);
				pthread_join(map_thread[count_for_map], (void**)&status);
				count_for_map++;
				if(count_for_map == num_mappers){
					count_for_map = 0;
				}*/
			}
		}
		//Read all files in the folder and call map function
		closedir(dp);


		for(i=0;i<nr_partition;i++){
			for(j=i;j<nr_partition;j++){
				if((shuffleMap[i]!=NULL)&&(shuffleMap[j]!=NULL)&&(strcmp(shuffleMap[i]->Key, shuffleMap[j]->Key)>0)){
					MAP **temp = (MAP**)malloc(sizeof(MAP)*2);
					temp[0] = shuffleMap[i];
					temp[1] = shuffleMap[j];
					shuffleMap[j] = temp[0];
					shuffleMap[i] = temp[1];
				}
			}
		}
		//sorting

		//MultipleArg *multiple_arg;
		//multiple_arg = (MultipleArg*)malloc(sizeof(MultipleArg));

		for(i = 0 ; i < nr_partition ; i++){
			if(count_partition[i] == 1){
				reduce(shuffleMap[i]->Key, Put_next, i);
				/*multiple_arg->get = &Put_next; 
				strcpy(multiple_arg->key, shuffleMap[i]->Key);
				multiple_arg->num = i;
				
				pthread_mutex_lock(&sync_mutex);
				if(pthread_create(&reduce_thread[count_for_reduce], NULL, t_function2, multiple_arg)<0){
					printf("error\n");
				}
				pthread_cond_wait(&sync_cond, &sync_mutex);
				pthread_mutex_unlock(&sync_mutex);

				pthread_join(reduce_thread[count_for_reduce], (void**)&status);
				count_for_reduce++;
				if(count_for_reduce == num_reducers){
					count_for_reduce = 0;
				}*/
			}
		}
		//reduce
	}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	if(input_long_unbalanced == 1){
		DIR* dp = NULL;
		struct dirent* entry = NULL;
		
		if((dp = opendir("./input_long_unbalanced")) == NULL){
			printf("Failed to open file\n");
		}//open directory

		//pthread_mutex_init(&sync_mutex, NULL);
		//pthread_cond_init(&sync_cond, NULL);

		while(((entry = readdir(dp)) != NULL)){
			if(strcmp(entry->d_name, ".")&&strcmp(entry->d_name, "..")){
				strcpy(temp, "./input_long_unbalanced/");
				strcat(temp,entry->d_name);
				map(temp);
				/*pthread_mutex_lock(&sync_mutex);
				if(pthread_create(&map_thread[count_for_map], NULL, t_function, (void*)&temp)<0){
					printf("error\n");
				}
				pthread_cond_wait(&sync_cond, &sync_mutex);
				pthread_mutex_unlock(&sync_mutex);
				pthread_join(map_thread[count_for_map], (void**)&status);
				count_for_map++;
				if(count_for_map == num_mappers){
					count_for_map = 0;
				}*/
			}
		}
		//Read all files in the folder and call map function
		closedir(dp);

		for(i=0;i<nr_partition;i++){
			for(j=i;j<nr_partition;j++){
				if((shuffleMap[i]!=NULL)&&(shuffleMap[j]!=NULL)&&(strcmp(shuffleMap[i]->Key, shuffleMap[j]->Key)>0)){
					MAP **temp = (MAP**)malloc(sizeof(MAP)*2);
					temp[0] = shuffleMap[i];
					temp[1] = shuffleMap[j];
					shuffleMap[j] = temp[0];
					shuffleMap[i] = temp[1];
				}
			}
		}
		//sorting

		//MultipleArg *multiple_arg;
		//multiple_arg = (MultipleArg*)malloc(sizeof(MultipleArg));

		for(i = 0 ; i < nr_partition ; i++){
			if(count_partition[i] == 1){
				reduce(shuffleMap[i]->Key, Put_next, i);
				/*multiple_arg->get = &Put_next; 
				strcpy(multiple_arg->key, shuffleMap[i]->Key);
				multiple_arg->num = i;
				
				pthread_mutex_lock(&sync_mutex);
				if(pthread_create(&reduce_thread[count_for_reduce], NULL, t_function2, multiple_arg)<0){
					printf("error\n");
				}
				pthread_cond_wait(&sync_cond, &sync_mutex);
				pthread_mutex_unlock(&sync_mutex);

				pthread_join(reduce_thread[count_for_reduce], (void**)&status);
				count_for_reduce++;
				if(count_for_reduce == num_reducers){
					count_for_reduce = 0;
				}*/
			}
		}
		//reduce
	}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
