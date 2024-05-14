#ifndef _RBH_REALTIME_RECORD_READER_H
#define _RBH_REALTIME_RECORD_READER_H

#include <stdio.h>
#include <limits.h>
#include <unistd.h>
#include <semaphore.h>
#include <stdbool.h>
#include <pthread.h>

#include "db_schema.h"
#include "rbh_misc.h"

// TODO:
// Let DBUF_MAX_SIZE be configured by autogen build system
#ifndef DBUF_MAX_SIZE
#define DBUF_MAX_SIZE 256
#endif

#ifndef RR_BUF_SIZE
#define RR_BUF_SIZE 128
#endif

#define HASH_SIZE 256   // Adjust based on needs

#define PRODUCER 0
#define CONSUMER 1

#define EVICTION_WM 85

enum op_type {
    POSIX_OPEN,            
    POSIX_OPEN64,            
    POSIX___OPEN_2,          
    POSIX_OPENAT,            
    POSIX_OPENAT64,         
    POSIX_READ,              
    POSIX_WRITE,
    POSIX_AIO_READ,
    POSIX_AIO_READ64,
    POSIX_AIO_WRITE,
    POSIX_AIO_WRITE64,             
    POSIX_PREAD,            
    POSIX_PWRITE,           
    POSIX_PREAD64,           
    POSIX_PWRITE64,          
    POSIX_READV,            
    POSIX_PREADV,           
    POSIX_PREADV64,          
    POSIX_PREADV2,           
    POSIX_PREADV64V2,        
    POSIX_WRITEV,          
    POSIX_PWRITEV,           
    POSIX_PWRITEV64,         
    POSIX_PWRITEV2,          
    POSIX_PWRITEV64V2,      
    POSIX_CREATE,	    
    POSIX_CREATE64,	    
    POSIX_CLOSE,
    POSIX_AIO_RETURN,
    POSIX_AIO_RETURN64,         
};

/* A structure storing records for real-time profiling 
/* regarding open, read, write methods. Filename is filled only by open call.
/* pid, fd is given for every open, read, write call. As file descriptor is not
/* unique across multiple processes running simultaneously,
/* use pid, fd altogether to identify the target file of read, write */

typedef struct realtime_record
{
    char path[PATH_MAX];                // path
    enum op_type type;                  // posix operation type
    unsigned long long size;            // the size of byte read/write. Has valid value only if op_type is read/write
} realtime_record_t;

typedef struct head {
    struct sm_segment* init_seg;
    struct sm_segment* curr_producer_seg;
    struct sm_segment* curr_consumer_seg;
    pthread_rwlock_t rwlock;
    unsigned int next_seg_idx;
} head_t;

typedef struct sm_segment {
    unsigned int seg_idx;
    struct realtime_record_buf[RR_BUF_SIZE];
    unsigned int role:1; 
    pthread_mutex_t l;
    struct sm_segment *next;
} sm_segment_t;

typedef struct promotion_candidate_item {
    entry_id_t fid;
    bool promotable;
    bool is_aio_read;                            // for POSIX_AIO_RETURUN type operation
    unsigned long long size;                            // file size
    unsigned long long rbyte;                           // read amount after it selected as promotion candidate
    unsigned long long wbyte;                           // written amount after it selected as promotion candidate
    promotion_candidate_item_t *prev, *next;  
    promotion_candidate_item_t *hash_prev, *hash_next;
} promotion_candidate_item_t;

typedef struct promotion_candidate_list {
    promotion_candidate_item_t* head;
    promotion_candidate_item_t* tail;
    promotion_candidate_item_t* hashtable[HASH_SIZE];
    pthread_mutex_t lock;     
} promotion_candidate_list_t;

typedef struct pcc_item {
    entry_id_t fid;                                 // lu_fid of the file 
    bool evictable;                          // whether it is evictable
    bool is_aio_read;                        // for POSIX_AIO_RETURN type operation
    unsigned long long size;                        // file size
    unsigned long long rbyte;                       // read amount after cached in PCC
    unsigned long long wbyte;                       // written amount after cached in PCC
    struct pcc_item_t *prev, *next;                 // for lru list
    struct pcc_item_t *hash_prev, *hash_next;       // for bucket list
} pcc_item_t;

typedef struct pcc {
    pcc_item_t* head;
    pcc_item_t* tail;
    pcc_item_t* hashtable[HASH_SIZE];
    unsigned long long capacity_remaining;
    pthread_mutex_t lock;
} pcc_t;

extern pcc_t *cache;
extern promotion_candidate_list_t *p_list;

void realtime_record_reader_start();
#endif
