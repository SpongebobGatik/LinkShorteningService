#define _CRT_SECURE_NO_WARNINGS
#include "table.h" 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#define MAX_SIZE 100000 

int countWordsInFileTable(const char* filename) {
	FILE* file = fopen(filename, "r");
	int count = 0;
	char word[10000];
	while (fscanf(file, "%s", word) != EOF) {
		count++;
	}
	fclose(file);
	return count;
}

HashTable* initHashTable() {
	HashTable* ht = (HashTable*)malloc(sizeof(HashTable));
	ht->nodes = (NodeHashTable**)malloc(MAX_SIZE * sizeof(NodeHashTable*));
	ht->count = 0;
	for (int i = 0; i < MAX_SIZE; i++) {
		ht->nodes[i] = NULL;
	}
	return ht;
}

int calculateHashT(const char* element) {
	int hash = 0;
	for (int i = 0; element[i] != '\0'; i++) {
		hash = 31 * hash + element[i];
	}
	return abs(hash) % MAX_SIZE;
}

void HSET(HashTable* ht, char* key, char* value) {
	int hash = calculateHashT(key);
	NodeHashTable* newNode = (NodeHashTable*)malloc(sizeof(NodeHashTable));
	newNode->key = _strdup(key);
	newNode->element = _strdup(value);
	newNode->next = NULL;
	newNode->prev = NULL;
	NodeHashTable* current = ht->nodes[hash];
	while (current != NULL) {
		if (strcmp(current->key, key) == 0) { 
			free(newNode->key);
			free(newNode->element);
			free(newNode);
			return;
		}
		if (current->next == NULL) {
			break;
		}
		current = current->next;
	}
	if (current == NULL) {
		ht->nodes[hash] = newNode;
	}
	else {
		current->next = newNode;
		newNode->prev = current;
	}
	ht->count++;
}

char* HGET(HashTable* ht, const char* key) {
	int hash = calculateHashT(key);
	NodeHashTable* current = ht->nodes[hash];
	while (current != NULL) {
		if (strcmp(current->key, key) == 0) {
			return current->element;
		}
		current = current->next;
	}
	return NULL;
}

void HDEL(HashTable* ht, const char* key) {
	int hash = calculateHashT(key);
	NodeHashTable* current = ht->nodes[hash];
	NodeHashTable* nodeToRemove = NULL;

	while (current != NULL) {
		if (strcmp(current->key, key) == 0) {
			nodeToRemove = current;
			break;
		}
		current = current->next;
	}

	if (nodeToRemove != NULL) {
		if (nodeToRemove->prev != NULL) {
			nodeToRemove->prev->next = nodeToRemove->next;
		}
		else {
			ht->nodes[hash] = nodeToRemove->next;
		}
		if (nodeToRemove->next != NULL) {
			nodeToRemove->next->prev = nodeToRemove->prev;
		}
		free(nodeToRemove->key);
		free(nodeToRemove->element);
		free(nodeToRemove);
		ht->count--;
	}
}

void saveToFileTable(HashTable* hashtable, const char* filename, const char* basename, int* pos1, int* pos2, int* status) {
	FILE* file = fopen(filename, "r");
	FILE* tempFile = fopen("temp.data", "w");
	int ch;
	int count = 0;
	fseek(file, 0, SEEK_SET);
	fseek(tempFile, 0, SEEK_SET);
	while ((ch = fgetc(file)) != EOF) {
		fputc(ch, tempFile);
		if (ftell(tempFile) == *pos1 || (ftell(tempFile) == *pos1 - 2 && *status == 2)) {
			if (hashtable->count == 1 && *status == 2) {
				fprintf(tempFile, "\t");
			}
			for (int i = 0; i < MAX_SIZE; i++) {
				NodeHashTable* currentNode = hashtable->nodes[i];
				while (currentNode != NULL) {
					fprintf(tempFile, "%s\t%s", currentNode->element, currentNode->key);
					count++;
					if (count != hashtable->count) {
						fprintf(tempFile, "\t");
					}
					else {
						fprintf(tempFile, "\n");
						break;
					}
					currentNode = currentNode->next;
				}
			}
			if (*status == 1) {
				fseek(tempFile, *pos1 - 1, SEEK_SET);
				fprintf(tempFile, "\n");
			}
			fseek(file, *pos2, SEEK_SET);
		}
	}
	fclose(file);
	fclose(tempFile);
	remove(filename);
	rename("temp.data", filename);
}

HashTable* loadFromFileTable(const char* filename, const char* basename, int* pos1, int* pos2, int* status) {
	FILE* file = fopen(filename, "r");
	if (file == NULL) {
		return NULL;
	}
	int num_lines = countWordsInFileTable(filename);
	char** line = malloc(num_lines * sizeof(char*));
	for (int i = 0; i < num_lines; i++) line[i] = malloc(10000 * sizeof(char));
	HashTable* hashtable = initHashTable();
	int tempory = 0;
	int tempory2 = 0;
	int temp1 = 0;
	int temp2 = 0;
	char c = '1';
	for (int i = 0; i < num_lines; ++i) {
		fscanf(file, "%s", line[i]);
		c = getc(file);
		if (c == '\n') {
			tempory2 = ftell(file);
		}
		if (!strcmp(line[i], basename) && (tempory2 == ftell(file) || tempory2 == ftell(file) - strlen(line[i]) - 1 || i == 0)) {
			tempory = 1;
			*pos1 = ftell(file);
			*pos2 = strlen(line[i]);
			temp1 = i + 1;
		}
		if (c == '\n' && tempory == 1) {
			temp2 = i;
			*pos2 = ftell(file);
			tempory = 0;
		}
		if (feof(file))
			break;
	}
	if (temp1 + 1 == temp2) *status = 1;
	if (temp1 == temp2 + 1) *status = 2;
	while (temp1 < temp2) {
		char* value = line[temp1];
		char* key = line[temp1 + 1];
		HSET(hashtable, key, value);
		temp1 += 2;
	}
	fclose(file);
	for (int i = 0; i < num_lines; i++) {
		free(line[i]);
	}
	free(line);
	return hashtable;
}