#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <dirent.h>
#include "mapreduce.h"

char temp[1000];
//characters for file name

int *count_partition;
int nr_partition;

Partitioner partitiontemp;

//data structure
struct MAP{
	int ref;
	char Key[50];
	char Value[5];
	struct MAP *next_node;
};
typedef struct MAP MAP;
//struct MAP has data and pointer to next node


//initializing structure

void MAP_add(MAP** root, char* key, char* value){
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
	return;
}
//add node

MAP *tempMap;
MAP **shuffleMap;

char* Put_next(char *key, int partition_number){
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
	return temp;
}




void MR_Emit(char *key, char *value){
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
	
}

void MR_Run(int argc, char *argv[], Mapper map, int num_mappers, Reducer reduce, int num_reducers, Partitioner partition){
	int input_simple = 0, input_long_balanced = 0, input_long_unbalanced = 0;
	//init variable

	partitiontemp = partition;

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
	int i, j;
	for(i = 0; i < nr_partition; i++){
		count_partition[i] = 0;
	}
	//init count_partition to set all value by 0.

	shuffleMap=(MAP**)malloc(sizeof(MAP)*nr_partition);

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	if(input_simple == 1){
		DIR* dp = NULL;
		struct dirent* entry = NULL;
		
		if((dp = opendir("./input_simple")) == NULL){
			printf("Failed to open file\n");
		}//open directory
		
		while(((entry = readdir(dp)) != NULL)){
			if(strcmp(entry->d_name, ".")&&strcmp(entry->d_name, "..")){
				strcpy(temp, "./input_simple/");
				strcat(temp,entry->d_name);
				map(temp);
			}
		}
		//Read all files in the folder and call map function

		closedir(dp);

		MAP **temp = (MAP**)malloc(sizeof(MAP)*2);

		for(i=0;i<nr_partition;i++){
			for(j=i;j<nr_partition;j++){
				if((shuffleMap[i]!=NULL)&&(shuffleMap[j]!=NULL)&&(strcmp(shuffleMap[i]->Key, shuffleMap[j]->Key)>0)){
					temp[0] = shuffleMap[i];
					temp[1] = shuffleMap[j];
					shuffleMap[j] = temp[0];
					shuffleMap[i] = temp[1];
				}
			}
		}
		//sorting

		for(i = 0 ; i < nr_partition ; i++){
			if(count_partition[i] == 1){
				reduce(shuffleMap[i]->Key, Put_next, i);
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
		
		while(((entry = readdir(dp)) != NULL)){
			if(strcmp(entry->d_name, ".")&&strcmp(entry->d_name, "..")){
				strcpy(temp, "./input_long_balanced/");
				strcat(temp,entry->d_name);
				map(temp);
			}
		}
		//Read all files in the folder and call map function

		closedir(dp);

		MAP **temp = (MAP**)malloc(sizeof(MAP)*2);

		for(i=0;i<nr_partition;i++){
			for(j=i;j<nr_partition;j++){
				if((shuffleMap[i]!=NULL)&&(shuffleMap[j]!=NULL)&&(strcmp(shuffleMap[i]->Key, shuffleMap[j]->Key)>0)){
					temp[0] = shuffleMap[i];
					temp[1] = shuffleMap[j];
					shuffleMap[j] = temp[0];
					shuffleMap[i] = temp[1];
				}
			}
		}
		//sorting

		for(i = 0 ; i < nr_partition ; i++){
			if(count_partition[i] == 1){
				reduce(shuffleMap[i]->Key, Put_next, i);
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
		
		while(((entry = readdir(dp)) != NULL)){
			if(strcmp(entry->d_name, ".")&&strcmp(entry->d_name, "..")){
				strcpy(temp, "./input_long_unbalanced/");
				strcat(temp,entry->d_name);
				map(temp);
			}
		}
		//Read all files in the folder and call map function

		closedir(dp);

		MAP **temp = (MAP**)malloc(sizeof(MAP)*2);

		for(i=0;i<nr_partition;i++){
			for(j=i;j<nr_partition;j++){
				if((shuffleMap[i]!=NULL)&&(shuffleMap[j]!=NULL)&&(strcmp(shuffleMap[i]->Key, shuffleMap[j]->Key)>0)){
					temp[0] = shuffleMap[i];
					temp[1] = shuffleMap[j];
					shuffleMap[j] = temp[0];
					shuffleMap[i] = temp[1];
				}
			}
		}
		//sorting

		for(i = 0 ; i < nr_partition ; i++){
			if(count_partition[i] == 1){
				reduce(shuffleMap[i]->Key, Put_next, i);
			}
		}
		//reduce

	}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}
