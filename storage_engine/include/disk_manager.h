#ifndef DISK_MANAGER_H
#define DISK_MANAGER_H

#include "common.h"

/*
 * disk_manager.h — lowest layer of the storage engine
 *
 * Manages one .mydb binary file per table. All I/O goes through
 * pread/pwrite so we never load an entire file into memory.
 *
 * File layout:
 *   Page 0  : FileHeader (metadata — magic, version, page count, root page)
 *   Page 1+ : Data / index pages (each exactly PAGE_SIZE bytes)
 *
 * Page N starts at byte offset: N * PAGE_SIZE
 */

/* ------------------------------------------------------------------ */
/*  File header — stored in the first PAGE_SIZE bytes (page 0)         */
/* ------------------------------------------------------------------ */
typedef struct {
    uint32_t magic;         /* MYDB_MAGIC (0x4D594442) — sanity check on open */
    uint32_t version;       /* file format version, currently 1 */
    uint32_t num_pages;     /* total pages in file including page 0 */
    uint32_t root_page_no;  /* root page of the clustered B+ Tree */
    uint32_t auto_incr;     /* AUTO_INCREMENT counter for the table */
    /* remaining bytes padded to PAGE_SIZE so page 1 starts at the right offset */
    uint8_t  _pad[PAGE_SIZE - 20];
} FileHeader;

/* ------------------------------------------------------------------ */
/*  DiskManager — one instance per open table file                     */
/* ------------------------------------------------------------------ */
typedef struct {
    int      fd;            /* POSIX file descriptor */
    char     path[256];     /* full path to the .mydb file */
    uint32_t num_pages;     /* cached copy of FileHeader.num_pages */
} DiskManager;

/* ------------------------------------------------------------------ */
/*  Functions                                                           */
/* ------------------------------------------------------------------ */

/* Create a new .mydb file. Writes page 0 (FileHeader). Fails if file exists. */
int disk_create(DiskManager *dm, const char *path);

/* Open an existing .mydb file. Reads page 0 to populate dm->num_pages. */
int disk_open(DiskManager *dm, const char *path);

/* Close the file descriptor. */
int disk_close(DiskManager *dm);

/* Read page page_no into buf (caller must provide PAGE_SIZE bytes). */
int disk_read_page(DiskManager *dm, uint32_t page_no, uint8_t *buf);

/* Write buf to page page_no on disk. */
int disk_write_page(DiskManager *dm, uint32_t page_no, const uint8_t *buf);

/* Allocate a new page at the end of the file. Sets *new_page_no. */
int disk_alloc_page(DiskManager *dm, uint32_t *new_page_no);

/* Read the file header from page 0. */
int disk_read_header(DiskManager *dm, FileHeader *fh);

/* Write the file header back to page 0. */
int disk_write_header(DiskManager *dm, const FileHeader *fh);

/* Delete the .mydb file from disk (used by DROP TABLE). */
int disk_destroy(const char *path);

#endif /* DISK_MANAGER_H */
