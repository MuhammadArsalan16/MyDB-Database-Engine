#include "disk_manager.h"

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

/* ------------------------------------------------------------------ */
/*  Internal helpers                                                    */
/* ------------------------------------------------------------------ */

/* Write exactly n bytes to fd at offset. Returns MYDB_OK on success. */
static int pwrite_all(int fd, const void *buf, size_t n, off_t offset)
{
    ssize_t written = pwrite(fd, buf, n, offset);
    return (written == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

/* Read exactly n bytes from fd at offset. Returns MYDB_OK on success. */
static int pread_all(int fd, void *buf, size_t n, off_t offset)
{
    ssize_t got = pread(fd, buf, n, offset);
    return (got == (ssize_t)n) ? MYDB_OK : MYDB_ERR;
}

/* ------------------------------------------------------------------ */
/*  Public API                                                          */
/* ------------------------------------------------------------------ */

int disk_create(DiskManager *dm, const char *path)
{
    if (!dm || !path) return MYDB_ERR;

    /* O_EXCL ensures we fail if the file already exists */
    int fd = open(path, O_RDWR | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return MYDB_ERR;

    /* Build the file header — sizeof(FileHeader) == PAGE_SIZE due to _pad */
    FileHeader fh;
    memset(&fh, 0, sizeof(fh));
    fh.magic        = MYDB_MAGIC;
    fh.version      = 1;
    fh.num_pages    = 1;             /* only page 0 exists right now */
    fh.root_page_no = INVALID_PAGE;  /* no B+ Tree yet */
    fh.auto_incr    = 1;             /* AUTO_INCREMENT starts at 1 */

    if (pwrite_all(fd, &fh, sizeof(fh), 0) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }

    snprintf(dm->path, sizeof(dm->path), "%s", path);
    dm->fd        = fd;
    dm->num_pages = 1;
    return MYDB_OK;
}

int disk_open(DiskManager *dm, const char *path)
{
    if (!dm || !path) return MYDB_ERR;

    int fd = open(path, O_RDWR);
    if (fd < 0) return MYDB_ERR;

    /* Read page 0 to verify the file and get the current page count */
    FileHeader fh;
    if (pread_all(fd, &fh, sizeof(fh), 0) != MYDB_OK) {
        close(fd);
        return MYDB_ERR;
    }

    if (fh.magic != MYDB_MAGIC) {   /* sanity check */
        close(fd);
        return MYDB_ERR;
    }

    snprintf(dm->path, sizeof(dm->path), "%s", path);
    dm->fd        = fd;
    dm->num_pages = fh.num_pages;
    return MYDB_OK;
}

int disk_close(DiskManager *dm)
{
    if (!dm || dm->fd < 0) return MYDB_ERR;
    close(dm->fd);
    dm->fd = -1;
    return MYDB_OK;
}

int disk_read_page(DiskManager *dm, uint32_t page_no, uint8_t *buf)
{
    if (!dm || !buf) return MYDB_ERR;
    if (page_no >= dm->num_pages) return MYDB_ERR;

    off_t offset = (off_t)page_no * PAGE_SIZE;
    return pread_all(dm->fd, buf, PAGE_SIZE, offset);
}

int disk_write_page(DiskManager *dm, uint32_t page_no, const uint8_t *buf)
{
    if (!dm || !buf) return MYDB_ERR;
    if (page_no >= dm->num_pages) return MYDB_ERR;

    off_t offset = (off_t)page_no * PAGE_SIZE;
    return pwrite_all(dm->fd, buf, PAGE_SIZE, offset);
}

int disk_alloc_page(DiskManager *dm, uint32_t *new_page_no)
{
    if (!dm || !new_page_no) return MYDB_ERR;

    uint32_t pno    = dm->num_pages;
    off_t    offset = (off_t)pno * PAGE_SIZE;

    /* Extend the file with a zeroed page */
    uint8_t zero[PAGE_SIZE];
    memset(zero, 0, PAGE_SIZE);
    if (pwrite_all(dm->fd, zero, PAGE_SIZE, offset) != MYDB_OK)
        return MYDB_ERR;

    dm->num_pages++;

    /* Persist updated page count in the file header */
    FileHeader fh;
    if (pread_all(dm->fd, &fh, sizeof(fh), 0) != MYDB_OK) return MYDB_ERR;
    fh.num_pages = dm->num_pages;
    if (pwrite_all(dm->fd, &fh, sizeof(fh), 0) != MYDB_OK) return MYDB_ERR;

    *new_page_no = pno;
    return MYDB_OK;
}

int disk_read_header(DiskManager *dm, FileHeader *fh)
{
    if (!dm || !fh) return MYDB_ERR;
    return pread_all(dm->fd, fh, sizeof(FileHeader), 0);
}

int disk_write_header(DiskManager *dm, const FileHeader *fh)
{
    if (!dm || !fh) return MYDB_ERR;
    dm->num_pages = fh->num_pages;  /* keep cache in sync */
    return pwrite_all(dm->fd, fh, sizeof(FileHeader), 0);
}

int disk_destroy(const char *path)
{
    if (!path) return MYDB_ERR;
    return (unlink(path) == 0) ? MYDB_OK : MYDB_ERR;
}
