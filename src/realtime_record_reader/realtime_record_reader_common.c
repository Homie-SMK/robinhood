#include <fcntl.h>      // For O_* constants
#include <sys/stat.h>   // For mode constants
#include <sys/mman.h>   // Shared memory and mmap
#include <sys/vfs.h>

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <stdatomic.h>
#include <errno.h>
#include <sysexits.h>

#include "realtime_record_reader_common.h"
#include "lustre_extended_types.h"
#include "policy_run.h"
#include "run_policies.h"
#include "list_mgr.h"

// Create HEAD shared memory object
void create_head_file(head_t *head, int *shm_fd, pthread_rwlockattr_t *attr) {

    // Create the shared memory object
    *shm_fd = shm_open("HEAD", O_CREAT | O_RDWR, 0666);
    if (shm_fd == -1) {
        perror("shm_open");
        exit(EX_OSERR);
    }

    // Configure the size of the shared memory object
    if (ftruncate(*shm_fd, sizeof(head_t)) == -1) {
        perror("ftruncate");
        exit(EX_OSERR);
    }

    // Memory map the shared memory object
    head = mmap(0, sizeof(head_t), PROT_READ | PROT_WRITE, MAP_SHARED, *shm_fd, 0);
    if (head == MAP_FAILED) {
        perror("mmap");
        exit(EX_OSERR);
    }

    // Initialize the read-write lock attribute
    pthread_rwlockattr_init(attr);
    pthread_rwlockattr_setpshared(attr, PTHREAD_PROCESS_SHARED);

    // Initialize the read-write lock
    pthread_rwlock_init(&head->rwlock, attr);

    // Initial structure setup
    head->init_seg = NULL;
    head->curr_producer_seg = NULL;
    head->curr_consumer_seg = NULL;
    head->next_seg_idx = 0;

    return;
}

// Cleanup HEAD shared memory object
void clean_head_file(head_t *head, int *shm_fd, pthread_rwlockattr_t *attr) {

    pthread_rwlock_destroy(&head->rwlock);
    pthread_rwlockattr_destroy(&attr);
    munmap(head, sizeof(head_t));
    close(shm_fd);
    shm_unlink("HEAD");

    return;
}

void create_pcc_cache(pcc_t * cache) {

    cache = (pcc_t*)malloc(sizeof(pcc_t));
    if (cache == NULL) {
        perror("Failed to allocate memory for LRU Cache");
        exit(EXIT_FAILURE);
    }
    pthread_mutex_init(cache->lock, NULL);
    cache->head = cache->tail = NULL;
    memset(cache->hashtable, 0, sizeof(cache->hashtable));
    cache->capacity_remaining = get_pcc_capacity_remaining();

    return;
}

void free_cache(pcc_t* cache) {
    pcc_item_t* current = cache->head;
    while (current) {
        pcc_item_t* temp = current;
        current = current->next;
        free(temp);
    }
    pthread_mutex_destroy(&cache->lock);
    free(cache);
}

void create_promotion_candidate_list(promotion_candidate_list_t *p_list) {

    p_list = (promotion_candidate_list_t *)malloc(sizeof(promotion_candidate_list_t));
    if (promotion_candidate_list_t == NULL) {
        perror("Failed to allocate memory for promotion candidate list");
        exit(EXIT_FAILURE);
    }
    p_list->head = p_list->tail = NULL;
    memset(p_list->hashtable, 0, sizeof(p_list->hashtable)); 
    pthread_mutex_init(&p_list->lock, NULL);

    return;
}

void free_promotion_candidate_list(promotion_candidate_list_t *p_list) {
    promotion_candidate_item_t* current = p_list->head;
    while(current) {
        promotion_candidate_item_t* temp = current;
        current = current->next;
        free(temp);
    }
    pthread_mutex_destroy(&p_list->lock);
    free(cache);
}

static pcc_item_t* create_new_cache_item(const entry_id_t *p_id, unsigned long long size) {
    
    pcc_item_t* item = (pcc_item_t*)malloc(sizeof pcc_item_t);
    if (item == NULL) {
        perror("Failed to allocate memory for item");
        exit(EXIT_FAILURE);
    }

    item->fid = *p_id;
    item->size = size;
    item->rbyte = 0;
    item->wbyte = 0;
    item->evictable = true;
    item->prev = item->next = NULL;
    item->hash_prev = item->hash_next = NULL;
    item->is_aio_read = true;

    return item;
}

promotion_candidate_item_t* create_new_promotion_candidate_item(const entry_id_t *p_id, unsigned long long size) {

    promotion_candidate_item_t *item = (promotion_candidate_item_t*)malloc(sizeof promotion_candidate_item_t);
    if (item == NULL) {
        perror("Failed to allocate memory for item");
        exit(EXIT_FAILURE);
    }

    item->fid = *p_id;
    item->size = size;
    item->rbyte = 0;
    item->wbyte = 0;
    item->evictable = true;
    item->prev = item->next = NULL;
    item->hash_prev = item->hash_next = NULL;
    item->is_aio_read = true;

    return item;
}

// TODO need to add pcc_path field in global_config
static unsigned long long get_pcc_capacity_remaining()
{
    struct statfs *stfs;
    char traverse_path[RBH_PATH_MAX];
    int rc;

    //rc = snprintf(traverse_path, RBH_PATH_MAX, "%s/.", global_config.fs_path);
    rc = snprintf(traverse_path, RBH_PATH_MAX, "/mnt/nvme/.");
    /*
    if (rc >= RBH_PATH_MAX) {
        DisplayLog(LVL_MAJOR, tag(pol), "Path too long: %s/.",
                   global_config.fs_path);
        return ENAMETOOLONG;
    }
    */
/*
    if (!CheckFSDevice(pol))
        return ENODEV;
*/
    /* retrieve filesystem usage info */
    if (statfs(traverse_path, stfs) != 0) {
        int err = errno;
/*
        DisplayLog(LVL_CRIT, tag(pol),
                   "Could not make a 'df' on %s: error %d: %s",
                   global_config.fs_path, err, strerror(err));
        return err;
*/
        DisplayLog(LVL_CRIT, tag(pol),
                   "Could not make a 'df' on /mnt/nvme: error %d: %s",
                   err, strerror(err));
        return err;
    }
    
    return stfs->f_bavail * stfs->f_bsize;
}

static unsigned long long get_right_wm_capacity() {
    struct statfs *stfs;
    char traverse_path[RBH_PATH_MAX];
    int rc;

    rc = snprintf(traverse_path, RBH_PATH_MAX, "/mnt/nvme/.");

    if (statfs(traverse_path, stfs) != 0) {
        int err = errno;
        DisplayLog(LVL_CRIT, tag(pol),
                   "Could not make a 'df' on /mnt/nvme: error %d: %s",
                   err, strerror(err));
        return err;
    }

    return (stfs->f_blocks / 100) * (100 - EVICTION_WM);
}

static unsigned int hash_with_fid(const struct entry_id_t *fid) {
    unsigned long hash = 5381;
    
    // Combine the sequence, object ID, and version fields into the hash
    hash = ((hash << 5) + hash) ^ (fid->f_seq & 0xFFFFFFFF);        // Low part of sequence
    hash = ((hash << 5) + hash) ^ (fid->f_seq >> 32);               // High part of sequence
    hash = ((hash << 5) + hash) ^ fid->f_oid;                       // Object ID
    hash = ((hash << 5) + hash) ^ fid->f_ver;                       // Version

    return hash % HASH_SIZE;
}

static int insert_to_cache(pcc_t *cache, pcc_item_t *item) {
    
    if (!cache || !item) {
        fprintf(stderr, "Invalid cache or item pointer\n");
        return -1;
    }

    // Insert into the LRU list at the front
    item->next = cache->head;
    item->prev = NULL;
    if (cache->head) {
        cache->head->prev = item;
    }
    cache->head = item;
    
    if (!cache->tail) {
        cache->tail = item; // If the list was empty, new item is also the tail
    }

    // Insert into hash table
    unsigned int idx = hash_with_fid(&item->fid);
    item->hash_next = cache->hashtable[idx];
    item->hash_prev = NULL;
    if (cache->hashtable[idx]) {
        cache->hashtable[idx]->hash_prev = item;
    }
    cache->hashtable[idx] = item;

    return 0;
}

static int remove_item_from_cache(pcc_t *cache, pcc_item_t *item) {
    if (item == NULL) return;  // Safety check

    // Disconnect the item from the list
    if (item->prev) {
        item->prev->next = item->next;
    } else {
        // Item is the head
        cache->head = item->next;
    }

    if (item->next) {
        item->next->prev = item->prev;
    } else {
        // Item is the tail
        cache->tail = item->prev;
    }

    // Now, item is fully disconnected
    item->next = NULL;
    item->prev = NULL;

    // Assuming the cache or some mechanism owns the item, free it
    free(item)
}

static void remove_item_from_promotion_candidate_list(promotion_candidate_list_t *p_list, promotion_candidate_item_t *item) {
    if (item == NULL) return;  // Safety check

    // Disconnect the item from the list
    if (item->prev) {
        item->prev->next = item->next;
    } else {
        // Item is the head
        p_list->head = item->next;
    }

    if (item->next) {
        item->next->prev = item->prev;
    } else {
        // Item is the tail
        p_list->tail = item->prev;
    }

    // Now, item is fully disconnected
    item->next = NULL;
    item->prev = NULL;

    // Assuming the cache or some mechanism owns the item, free it
    free(item);
}

static void move_to_front(pcc_item_t* item) {
    if (cache->head == item) return;  // Already at the front

    // Remove from current position
    if (item->prev) item->prev->next = item->next;
    if (item->next) item->next->prev = item->prev;
    if (cache->tail == item) cache->tail = item->prev; // Update tail if needed

    // Insert at front
    item->next = cache->head;
    item->prev = NULL;
    if (cache->head) cache->head->prev = item;
    cache->head = item;
    if (cache->tail == NULL) cache->tail = item;  // First node added
}

static int insert_to_promotion_list(promotion_candidate_list_t *p_list, promotion_candidate_item_t *item) {
    
    if (!p_list || !item) {
        fprintf(stderr, "Invalid cache or item pointer\n");
        return -1;
    }

    // Insert into the FIFO list at the front
    item->next = p_list->head;
    item->prev = NULL;
    if (p_list->head) {
        p_list->head->prev = item;
    }
    p_list->head = item;
    
    if (!p_list->tail) {
        p_list->tail = item; // If the list was empty, new item is also the tail
    }

    // Insert into hash table
    unsigned int idx = hash_with_fid(&item->fid);
    item->hash_next = p_list->hashtable[idx];
    item->hash_prev = NULL;
    if (p_list->hashtable[idx]) {
        p_list->hashtable[idx]->hash_prev = item;
    }
    p_list->hashtable[idx] = item;

    return 0;
}

static void handle_open(entry_id_t fid, realtime_record_t record) {
    
    pcc_item_t *item = cache->hashtable[hash_with_fid(fid)];
    int rc;

    if (item) {
        
        while(!entry_id_equal(fid, item->fid)) {
            item = item->hash_next;
            if(item == NULL) {
                goto not_in_cache;
            }
        }

        pthread_mutex_lock(&cache->lock);
        &item->evictable = false;
        pthread_mutex_unlock(&cache->lock);
        
        return 0;
    }

 not_in_cache:
    promotion_candidate_item_t *candidate = p_list->hashtable[hash_with_fid(fid)];

    if(candidate) {

        while(!entry_id_equal(fid, candidate->fid)) {
            candidate = candidate->hash_next;
            if(candidate == NULL) {
                goto not_in_list;
            }
        }

        pthread_mutex_lock(&list->lock);
        &candidate->promotable = false;
        pthread_mutex_unlock(&list->lock);

        return 0;
    }

 not_in_list:
    promotion_candidate_item_t *item = create_new_promotion_candidate_item(fid, record.size);    
    rc = insert_to_promotion_list(p_list, item);
    if (rc != 0) {
        DisplayLog(LVL_DEBUG, "add_to_promotion_list_error",
                    "Error occured while adding " DFID "to promotion list", PFID(fid));
    }

    return;
}

static void handle_read(entry_id_t fid, realtime_record_t record) {
    
    pcc_item_t *item = cache->hashtable[hash_with_fid(fid)];

    if (item) {
        while(!entry_id_equal(fid, item->fid)) {
            item = item->hash_next;
            if(item == NULL) {
                goto not_in_cache;
            }
        }

        pthread_mutex_lock(&cache->lock);
        &item->rbyte += record.size;
        move_to_front(item);
        pthread_mutex_unlock(&cache->lock);
        
        return;
    }
    
 not_in_cache:   
    promotion_candidate_item_t *candidate = p_list->hashtable[hash_with_fid(fid)];
    
    if(candidate) {

        while(!entry_id_equal(fid, candidate->fid)) {
            candidate = candidate->hash_next;
            if(candidate == NULL) {
                goto not_in_list;
            }
        }

        pthread_mutex_lock(&list->lock);
        &candidate->rbyte += record.size;
        pthread_mutex_unlock(&list->lock);

        return;
    }

 not_in_list:
    DisplayLog(LVL_DEBUG, "handle_read_error",
                    "Error occured while handling read type record: " DFID "", PFID(fid));
    return;
}

static void handle_write(entry_id_t fid, realtime_record_t record) {
    
    pcc_item_t *item = cache->hashtable[hash_with_fid(fid)];
    
    if (item) {

        while(!entry_id_equal(fid, item->fid)) {
            item = item->hash_next;
            if(item == NULL) {
                goto not_in_cache;
            }
        }

        pthread_mutex_lock(&cache->lock);
        &item->wbyte += record.size;
        &item->size += record.size;
        move_to_front(item);
        pthread_mutex_unlock(&cache->lock);

        return;
    }

 not_in_cache:
    promotion_candidate_item_t *candidate = p_list->hashtable[hash_with_fid(fid)];
    
    if (candidate) {

        while(!entry_id_equal(fid, candidate->fid)) {
            candidate = candidate->hash_next;
            if(candidate == NULL) {
                goto not_in_list;
            }
        }

        pthread_mutex_lock(&list->lock);
        &candidate->wbyte += record.size;
        &candidate->size += record.size;
        pthread_mutex_unlock(&list->lock);

        return;
    }
 
 not_in_list:
    DisplayLog(LVL_DEBUG, "handle_write_error",
                    "Error occured while handling write type record: " DFID "", PFID(fid));
    return;
}

static void handle_close(entry_id_t fid, realtime_record_t record) {
    
    pcc_item_t *item = cache->hashtable[hash_with_fid(fid)];
    
    if (item) {

        while(!entry_id_equal(fid, item->fid)) {
            item = item->hash_next;
            if(item == NULL) {
                goto not_in_cache;
            }
        }

        pthread_mutex_lock(&cache->lock);
        &item->evictable = true;
        pthread_mutex_unlock(&cache->lock);

        return;
    }

 not_in_cache:
    promotion_candidate_item_t *candidate = p_list->hashtable[hash_with_fid(fid)];
    
    if (candidate) {

        while(!entry_id_equal(fid, candidate->fid)) {
            candidate = candidate->hash_next;
            if(candidate == NULL) {
                goto not_in_list;
            }
        }

        pthread_mutex_lock(&list->lock);
        &candidate->promotable = true;
        pthread_mutex_unlock(&list->lock);

        return;
    }
 
 not_in_list:
    DisplayLog(LVL_DEBUG, "handle_close_error",
                    "Error occured while handling close type record: " DFID "", PFID(fid));
    return;
}

static void handle_aio_read(entry_id_t fid, realtime_record_t record) {
    
    pcc_item_t *item = cache->hashtable[hash_with_fid(fid)];
    
    if (item) {

        while(!entry_id_equal(fid, item->fid)) {
            item = item->hash_next;
            if(item == NULL) {
                goto not_in_cache;
            }
        }

        pthread_mutex_lock(&cache->lock);
        &item->is_aio_read = true;
        move_to_front(item);
        pthread_mutex_unlock(&cache->lock);

        return;
    }

 not_in_cache:
    promotion_candidate_item_t *candidate = p_list->hashtable[hash_with_fid(fid)];
    
    if (candidate) {

        while(!entry_id_equal(fid, candidate->fid)) {
            candidate = candidate->hash_next;
            if(candidate == NULL) {
                goto not_in_list;
            }
        }

        pthread_mutex_lock(&list->lock);
        &candidate->is_aio_read = true;
        pthread_mutex_unlock(&list->lock);

        return;
    }
 
 not_in_list:
    DisplayLog(LVL_DEBUG, "handle_aio_read_error",
                    "Error occured while handling aio_read type record: " DFID "", PFID(fid));
    return;
}

static void handle_aio_write(entry_id_t fid, realtime_record_t record) {
    
    pcc_item_t *item = cache->hashtable[hash_with_fid(fid)];
    
    if (item) {

        while(!entry_id_equal(fid, item->fid)) {
            item = item->hash_next;
            if(item == NULL) {
                goto not_in_cache;
            }
        }

        pthread_mutex_lock(&cache->lock);
        &item->is_aio_read = false;
        move_to_front(item);
        pthread_mutex_unlock(&cache->lock);

        return;
    }

 not_in_cache:
    promotion_candidate_item_t *candidate = p_list->hashtable[hash_with_fid(fid)];
    
    if (candidate) {

        while(!entry_id_equal(fid, candidate->fid)) {
            candidate = candidate->hash_next;
            if(candidate == NULL) {
                goto not_in_list;
            }
        }

        pthread_mutex_lock(&list->lock);
        &candidate->is_aio_read = false;
        pthread_mutex_unlock(&list->lock);

        return;
    }
 
 not_in_list:
    DisplayLog(LVL_DEBUG, "handle_aio_write_error",
                    "Error occured while handling aio_write type record: " DFID "", PFID(fid));
    return;
}

static void handle_aio_return(entry_id_t fid, realtime_record_t record) {
    
    pcc_item_t *item = cache->hashtable[hash_with_fid(fid)];
    
    if (item) {

        while(!entry_id_equal(fid, item->fid)) {
            item = item->hash_next;
            if(item == NULL) {
                goto not_in_cache;
            }
        }

        pthread_mutex_lock(&cache->lock);
        if(&item->aio_read) {
            &item->rbyte += record.size;
        } else {
            &item->wbyte += record.size;
            &item->size += record.size;
        }
        pthread_mutex_unlock(&cache->lock);

        return;
    }

 not_in_cache:
    promotion_candidate_item_t *candidate = p_list->hashtable[hash_with_fid(fid)];
    
    if (candidate) {

        while(!entry_id_equal(fid, candidate->fid)) {
            candidate = candidate->hash_next;
            if(candidate == NULL) {
                goto not_in_list;
            }
        }

        pthread_mutex_lock(&list->lock);
        if(&candidate->is_aio_read) {
            &candidate->rbyte += record.size;
        } else {
            &candidate->wbyte += record.size;
            &candidate->size += record.size;
        }
        pthread_mutex_unlock(&list->lock);

        return;
    }
 
 not_in_list:
    DisplayLog(LVL_DEBUG, "handle_aio_return_error",
                    "Error occured while handling aio_return type record: " DFID "", PFID(fid));
    return;
}

static void *realtime_record_reader_thr(void *arg) {
    
    head_t *head = (head_t *)arg;
    int rc;

    while (1) {
        rh_sleep(1); // Periodically execute every 1 second

        // Obtain read lock on the head structure to safely read the current consumer segment
        pthread_rwlock_rdlock(&head->rwlock);
        sm_segment_t *consumer_seg = head->curr_consumer_seg;
        pthread_rwlock_unlock(&head->rwlock);

        if (consumer_seg != NULL) {
            // Lock the segment for safe access
            // pthread_mutex_lock(&consumer_seg->l);

            // Process each realtime record in the buffer
            for (int i = 0; i < RR_BUF_SIZE; i++) {
                realtime_record_t record = consumer_seg->realtime_record_buf[i];

                // Assuming llapi_path2fid is implemented elsewhere
                entry_id_t fid;
                rc = Lustre_GetFidFromPath(record.path, &fid, NULL);

                // Operation specific processing (pseudo-code)
                switch (record.type) {
                    case POSIX_OPEN:
                    case POSIX_OPEN64:
                    case POSIX___OPEN_2:
                    case POSIX_OPENAT:
                    case POSIX_OPENAT64:
                    case POSIX_CREATE:
                    case POSIX_CREATE64:

                        struct stat statbuf;
                        if(stat(record.path, &statbuf) != 0) {
                            DisplayLog(LVL_CRIT, "stat_error", "failed to get stat of the file %s", record.path);
                            exit(EX_UNAVAILABLE)
                        }
                        record.size = statbuf.st_size;
                        // Handle 'open' operation
                        handle_open(fid, record);
                        break;
                    
                    case POSIX_READ:
                    case POSIX_PREAD:
                    case POSIX_PREAD64:
                    case POSIX_READV:
                    case POSIX_PREADV:
                    case POSIX_PREADV64:
                    case POSIX_PREADV2:
                    case POSIX_PREADV64V2:
                        // Handle 'read' operation
                        handle_read(fid, record);
                        break;

                    case POSIX_AIO_READ:
                    case POSIX_AIO_READ64:
                        // Handle 'aio_read' operation
                        handle_aio_read(fid, record);
                        break;
                    
                    case POSIX_WRITE:
                    case POSIX_PWRITE:
                    case POSIX_PWRITE64:
                    case POSIX_WRITEV:
                    case POSIX_PWRITEV:
                    case POSIX_PWRITEV64:
                    case POSIX_PWRITEV2:
                    case POSIX_PWRITEV64V2:
                        // Handle 'write' operation
                        handle_write(fid, record);
                        break;

                    case POSIX_AIO_WRITE:
                    case POSIX_AIO_WRITE64:
                        // Handle 'aio_write' operation
                        handle_aio_write(fid, record);
                        break;

                    case POSIX_CLOSE:
                        // Handle 'close' operation
                        handle_close(fid, record);
                        break;
                    
                    case POSIX_AIO_RETURN:
                    case POSIX_AIO_RETURN64:
                        // Handle 'aio_return' operation
                        handle_aio_return(fid, record);
                        break;
                    
                    default:
                        break;    
                }
            }

            // Unlock the segment after processing
            // pthread_mutex_unlock(&consumer_seg->l);
        }
    }

    return NULL;
}

static void promotion_thr(void *arg) {
    
    promotion_candidate_list_t *tmp_list;
    promotion_candidate_item_t *fk_item;
    unsigned long long tmp_size;
    promotion_candidate_item_t *tmp;


    while(1) {
        create_promotion_candidate_list(tmp_list);

        // Populate tmp_list for promotion
        tmp_size = 0;
        
        pthread_mutex_lock(&cache->lock);
        cache->capacity_remaining = get_pcc_capacity_remaining();
        pthread_mutex_unlock(&cache->lock);

        pthread_mutex_lock(&p_list->lock);
        tmp = p_list->head;

        while(tmp != NULL) {
            if(tmp->promotable) {
                tmp_size += tmp->size;
                if(tmp_size < cache->capacity_remaining) {
                    fk_item = create_new_promotion_candidate_item(&tmp->fid, 0);
                    insert_to_promotion_list(tmp_list, fk_item);
                } else {
                    remove_item_from_promotion_candidate_list(tmp_list, fk_item);
                    break;
                }
            }
            tmp = tmp->next;            
        }
        pthread_mutex_unlock(&p_list->lock);        

        tmp = tmp_list->head;
        while(tmp != NULL) {
            gchar cmd_in[1024];
            gint num_args;
            gchar **cmd_out;
            int rc;

            sprintf(cmd_in, "lfs pcc attach_fid -i 1 -m /mnt/lustre " DFID, PFID(tmp->fid));
            g_shell_parse_argv(cmd_in, &num_args, &cmd_out, NULL);
            rc = execute_shell_command(cmd_out, cb_stderr_to_log,
                                               (void *)LVL_DEBUG);
            if(rc != 0) {
                DisplayLog(LVL_DEBUG, "ExeShellCmdError", "Error occured while execute_shell_command:%s", cmd_out[0]);
            } else {
                pthread_mutex_lock(&p_list->lock);
                remove_item_from_promotion_candidate_list(p_list, tmp);
                pthread_mutex_unlock(&p_list->lock);
            }

            g_strfreev(cmd_out);

            tmp = tmp->next;
        }

        rh_sleep(1);
    }
}

static void eviction_thr(void *arg) {

    pcc_t *tmp_cache;
    pcc_item_t *fk_item;
    unsigned long long tmp_size, need_to_be_evicted_size;
    pcc_item_t *tmp;

    while(1) {
        create_pcc_cache(tmp_cache);

        // Populate tmp_cache for eviction
        tmp_size = 0;

        pthread_mutex_lock(cache->lock);
        need_to_be_evicted_size = get_right_wm_capacity() - get_pcc_capacity_remaining();
        pthread_mutex_unlock(cache->lock);

        pthread_mutex_lock(&cache->lock);
        tmp = tmp_cache->head;

        while(tmp != NULL) {
            if(tmp->evictable) {
                tmp_size += tmp->size;
                if(tmp_size < need_to_be_evicted_size) {
                    fk_item = create_new_cache_item(&tmp->fid, tmp->size);
                    insert_to_cache(tmp_cache, fk_item);
                } else {
                    remove_item_from_cache(tmp_cache, fk_item);
                    break;
                }
            }
            tmp = tmp->next;            
        }
        pthread_mutex_unlock(&p_list->lock); 

        tmp = tmp_list->head;
        while(tmp != NULL) {
            gchar cmd_in[1024];
            gint num_args;
            gchar **cmd_out;
            int rc;

            sprintf(cmd_in, "lfs pcc detach_fid /mnt/lustre " DFID, PFID(tmp->fid));
            g_shell_parse_argv(cmd_in, &num_args, &cmd_out, NULL);
            rc = execute_shell_command(cmd_out, cb_stderr_to_log,
                                               (void *)LVL_DEBUG);
            if(rc != 0) {
                DisplayLog(LVL_DEBUG, "ExeShellCmdError", "Error occured while execute_shell_command:%s", cmd_out[0]);
            } else {
                pthread_mutex_lock(&cache->lock);
                remove_item_from_promotion_candidate_list(p_list, tmp);
                pthread_mutex_unlock(&cache->lock);
            }
            g_strfreev(cmd_out);

            tmp = tmp->next;
        }

        rh_sleep(1);
    }
}

void realtime_record_reader_start(head_t *head, int *shm_fd, pthread_rwlockattr_t * attr) {

    pthread_t reader_thread_id;
    pthread_t promotion_thread_id;
    pthread_t eviction_thread_id;
    int rc;

    create_head_file(head, shm_fd, attr);

    rc = pthread_create(&reader_thread_id, NULL, realtime_record_reader_thr, head);
    if(rc != 0) {
        DisplayLog(LVL_CRIT, "realtime record reader failed", 
                   "Error %d creating realtime reader thread: %s", rc, strerror(rc));
    }
    
    rc = pthread_create(&promotion_thread_id, NULL, promotion_thr, NULL);
    if(rc != 0) {
        DisplayLog(LVL_CRIT, "promotion thread failed", 
                   "Error %d creating promotion thread: %s", rc, strerror(rc));
    }

    rc = pthread_create(&eviction_thread_id, NULL, eviction_thr, NULL);
    if(rc != 0) {
        DisplayLog(LVL_CRIT, "eviction thread failed", 
                   "Error %d creating eviction thread: %s", rc, strerror(rc));
    }
}
