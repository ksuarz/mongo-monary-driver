// Monary - Copyright 2011-2014 David J. C. Beach
// Please see the included LICENSE.TXT and NOTICE.TXT for licensing information.

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mongoc.h"
#include "bson.h"

#ifndef NDEBUG
#define DEBUG(format, ...) \
    fprintf(stderr, "[DEBUG] %s:%i " format "\n", __FILE__, __LINE__, ##__VA_ARGS__)
#else
#define DEBUG(...)
#endif

#define DEFAULT_MONGO_HOST "127.0.0.1"
#define DEFAULT_MONGO_PORT 27017
#define MONARY_MAX_NUM_COLUMNS 1024
#define MONARY_MAX_NAME_LENGTH 1024
#define MONARY_MAX_QUERY_LENGTH 4096

// TODO
enum {
    TYPE_UNDEFINED = 0,
    TYPE_OBJECTID = 1,
    TYPE_BOOL = 2,
    TYPE_INT8 = 3,
    TYPE_INT16 = 4,
    TYPE_INT32 = 5,
    TYPE_INT64 = 6,
    TYPE_UINT8 = 7,
    TYPE_UINT16 = 8,
    TYPE_UINT32 = 9,
    TYPE_UINT64 = 10,
    TYPE_FLOAT32 = 11,
    TYPE_FLOAT64 = 12,
    TYPE_DATETIME = 13, // BSON date-time, seconds since the UNIX epoch (uint64 storage)
    TYPE_UTF8 = 14,     // each record is (type_arg) chars in length
    TYPE_BINARY = 15,   // each record is (type_arg) bytes in length
    TYPE_DOCUMENT = 16, // BSON subdocument as binary; each record is type_arg bytes
    LAST_TYPE = 16,
    TYPE_ARRAY = 17 // Where should we throw this in?
};

// XXX Some of these are already defined and Clang dislikes it
typedef bson_oid_t OBJECTID;
typedef int64_t DATETIME;
#ifndef WIN32
typedef char BOOL;
typedef char INT8;
#endif
typedef short INT16;
typedef int INT32;
typedef int64_t INT64;
typedef unsigned char UINT8;
typedef unsigned short UINT16;
typedef unsigned int UINT32;
// typedef unsigned uint64_t UINT64;
typedef unsigned long long UINT64;
typedef float FLOAT32;
typedef double FLOAT64;
typedef char* UTF8;

/**
 * Makes a new connection to a MongoDB server. It also allows for
 * authentication to a database using a username and password. This combines
 * the old behaviors of monary_connect and monary_authenticate.
 *
 * A new feature is the options parameter, which allows you to specify extra
 * options to the MongoDB connection URI.
 *
 * @param host The name of the host to connect to; if it is NULL, then it
 * uses the default MongoDB hostname.
 * @param port The port to connect to on the host; if the port is 0, then it
 * uses the default MongoDB port number.
 * @param db The database to connect to. If this is not specified but username
 * and password credentials exist, then it defaults to the admin database as
 * per mongoc_uri(7).
 * @param user An optional username.
 * @param pass A password for the user.
 * @param options Additional options for the MongoDB connection URI.
 *
 * @return A pointer to a mongoc_client_t. Note that this object is NOT thread
 * safe and can only be used from one thread at a time. See mongoc_client(3).
 */
mongoc_client_t* monary_connect(const char* host,
                                int port,
                                const char* db,
                                const char* user,
                                const char* pass,
                                const char* options)
{
    // XXX: Draft only, but let's worry about making code pretty after it works
    // TODO: Use malloc and snprintf instead of asprntf before pushing to master
    char *uri, *userpass;

    // Hostname and portname
    if(host == NULL) {
        host = DEFAULT_MONGO_HOST;
    }
    if(port == 0) {
        port = DEFAULT_MONGO_PORT;
    }

    // Specify a database name; otherwise, fall back to default
    if (!db) {
        db = "";
    }

    // Possible username and password combinations
    if (user && !pass) {
        userpass = asprintf(&userpass, "%s@", user);
    }
    else if(user && pass) {
        userpass = asprintf(&userpass, "%s:%s@", user, pass);
    }
    else {
        // A single NUL character (the empty string)
        userpass = (char *) calloc(1, sizeof(char));
    }

    // Additional URI options
    if (!options) {
        options = "";
    }

    asprintf(&uri, "mongodb://%s%s:%i/%s?%s", userpass, host, port, db, options);
    DEBUG("attempting connection to: '%s' port %i", host, port);
    mongoc_client_t* client = mongoc_client_new(uri);

    if(client) {
        DEBUG("connected successfully");
        return client;
    } else {
        DEBUG("an error occurred when attempting to connect to %s\n", uri);
        return NULL;
    }

    // Cleanup
    free(userpass);
    free(uri);
}

/**
 * Makes a new connection to a MongoDB server and database.
 *
 * @param uri A MongoDB URI, as per mongoc_uri(7).
 *
 * @return A pointer to a mongoc_client_t, or NULL if the connection attempt
 * was unsuccessful.
 */
mongoc_client_t *monary_connect_uri(const char *uri) {
    mongoc_client_t *client;
    if (!uri) {
        return NULL;
    }

    DEBUG("Attempting connection to: %s", uri);
    client = mongoc_client_new(uri);
    if (client) {
        DEBUG("Connection successful.");
    }
    else {
        DEBUG("An error occurred while attempting to connect to %s\n", uri);
    }
    return client;
}

/**
 * Destroys all resources associated with the client.
 */
void monary_disconnect(mongoc_client_t* client)
{
    mongoc_client_destroy(client);
}

/**
 * Holds the storage for an array of objects.
 *
 * @memb field The name of the field in the document.
 * @memb type The BSON type identifier, as specified by the Monary type enum.
 * @memb type_arg If type is binary, UTF-8, or document, type_arg specifies the
 * width of the field in bytes.
 * @memb storage A pointer to the location of the "array" in memory. In the
 * Python side of Monary, this points to the start of the NumPy array.
 * @memb mask A pointer to the the "masked array." This is the internal
 * representation of the NumPy ma.array, which corresponds one-to-one to the
 * storage array. A value is masked if and only if an error occurs while
 * loading memory from MongoDB.
 */
typedef struct monary_column_item
{
    char* field;
    unsigned int type;
    unsigned int type_arg;
    void* storage;
    unsigned char* mask;
} monary_column_item;

/**
 * Represents a collection of arrays.
 *
 * @memb num_columns The number of arrays to track, one per field.
 * @memb num_rows The number of elements per array. (Specifically, each
 * monary_column_item.storage contains num_rows elements.)
 * @memb columns A pointer to the first array.
 */
typedef struct monary_column_data
{
    unsigned int num_columns;
    unsigned int num_rows;
    monary_column_item* columns;
} monary_column_data;

/**
 * A MongoDB cursor augmented with Monary column data.
 */
typedef struct monary_cursor {
    mongoc_cursor_t* mcursor;
    monary_column_data* coldata;
} monary_cursor;

/**
 * Allocates heap space for data storage.
 *
 * @param num_columns The number of fields to store (that is, the number of
 * internal monary_column_item structs tracked by the column data structure).
 * Cannot exceed MONARY_MAX_NUM_COLS.
 * @param num_rows The lengths of the arrays managed by each column item.
 *
 * @return A pointer to the newly-allocated column data.
 */
monary_column_data* monary_alloc_column_data(unsigned int num_columns,
                                             unsigned int num_rows)
{
    // XXX malloc failures could throw an exception
    if(num_columns > MONARY_MAX_NUM_COLUMNS) { return NULL; }
    monary_column_data* result = (monary_column_data*) malloc(sizeof(monary_column_data));
    monary_column_item* columns = (monary_column_item*) calloc(num_columns, sizeof(monary_column_item));
    if (!result || !columns) {
        return NULL;
    }
    result->num_columns = num_columns;
    result->num_rows = num_rows;
    result->columns = columns;

    return result;
}

int monary_free_column_data(monary_column_data* coldata)
{
    if(coldata == NULL || coldata->columns == NULL) { return 0; }
    for(int i = 0; i < coldata->num_columns; i++) {
        monary_column_item* col = coldata->columns + i;
        if(col->field != NULL) { free(col->field); }
    }
    free(coldata->columns);
    free(coldata);
    return 1;
}

/**
 * Sets the field value for a particular column and item. This is referenced by
 * Monary._make_column_data.
 *
 * @param coldata A pointer to the column data to modify.
 * @param colnum The number of the column item within the table to modify
 * (representing one data field). Columns are indexed starting from zero.
 * @param field The new name of the column item. Cannot exceed
 * MONARY_MAX_NAME_LENGTH characters in length.
 * @param type The new type of the item.
 * @param type_arg For UTF-8, binary and BSON types, specifies the size of the
 * data.
 * @param storage A pointer to the new location of the data in memory, which
 * cannot be NULL. Note that this does not free(3) the previous storage
 * pointer.
 * @param mask A pointer to the new masked array, which cannot be NULL. It is a
 * programming error to have a masked array different in length from the
 * storage array.
 *
 * @return 1 if the modification was performed successfully; 0 otherwise.
 */
int monary_set_column_item(monary_column_data* coldata,
                           unsigned int colnum,
                           const char* field,
                           unsigned int type,
                           unsigned int type_arg,
                           void* storage,
                           unsigned char* mask)
{
    if(coldata == NULL) { return 0; }
    if(colnum >= coldata->num_columns) { return 0; }
    if(type == TYPE_UNDEFINED || type > LAST_TYPE) { return 0; }
    if(storage == NULL) { return 0; }
    if(mask == NULL) { return 0; }
    
    int len = strlen(field);
    if(len > MONARY_MAX_NAME_LENGTH) { return 0; }
    
    monary_column_item* col = coldata->columns + colnum;

    col->field = (char*) malloc(len + 1);
    if (!(col->field)) {
        return 0;
    }
    strcpy(col->field, field);
    
    col->type = type;
    col->type_arg = type_arg;
    col->storage = storage;
    col->mask = mask;

    return 1;
}

int monary_load_oid_value(const bson_iter_t* bsonit,
                          monary_column_item* citem,
                          int idx)
{
    if (BSON_ITER_HOLDS_OID(bsonit)) {
        const OBJECTID* oid = bson_iter_oid(bsonit);
        OBJECTID* oidloc = ((OBJECTID*) citem->storage) + idx;

        // Move over all 12 bytes. Faster than memcpy(3)?
        int i;
        for (i = 0; i < 12; i++) {
            oidloc->bytes[i] = oid->bytes[i];
        }
        return 1;
    } else {
        return 0;
    }
}

int monary_load_bool_value(const bson_iter_t* bsonit,
                           monary_column_item* citem,
                           int idx)
{
    BOOL value = bson_iter_bool(bsonit);
    ((BOOL*) citem->storage)[idx] = value;
    return 1;
}

#define MONARY_DEFINE_NUM_LOADER(FUNCNAME, NUMTYPE, BSONFUNC, VERIFIER) \
int FUNCNAME (const bson_iter_t *bsonit,                                \
              monary_column_item *citem,                                \
              int idx)                                                  \
{                                                                       \
    if (VERIFIER(bsonit)) {                                             \
        NUMTYPE value = BSONFUNC(bsonit);                               \
        ((NUMTYPE*) citem->storage)[idx] = value;                       \
        return 1;                                                       \
    } else {                                                            \
        return 0;                                                       \
    }                                                                   \
}

// Signed integers
MONARY_DEFINE_NUM_LOADER(monary_load_int8_value, INT8, bson_iter_int32, BSON_ITER_HOLDS_INT32)
MONARY_DEFINE_NUM_LOADER(monary_load_int16_value, INT16, bson_iter_int32, BSON_ITER_HOLDS_INT32)
MONARY_DEFINE_NUM_LOADER(monary_load_int32_value, INT32, bson_iter_int32, BSON_ITER_HOLDS_INT32)
MONARY_DEFINE_NUM_LOADER(monary_load_int64_value, INT64, bson_iter_int64, BSON_ITER_HOLDS_INT64)

// Unsigned integers
MONARY_DEFINE_NUM_LOADER(monary_load_uint8_value, UINT8, bson_iter_int32, BSON_ITER_HOLDS_INT32)
MONARY_DEFINE_NUM_LOADER(monary_load_uint16_value, UINT16, bson_iter_int32, BSON_ITER_HOLDS_INT32)
MONARY_DEFINE_NUM_LOADER(monary_load_uint32_value, UINT32, bson_iter_int32, BSON_ITER_HOLDS_INT32)
MONARY_DEFINE_NUM_LOADER(monary_load_uint64_value, UINT64, bson_iter_int64, BSON_ITER_HOLDS_INT64)

// Floating point
MONARY_DEFINE_NUM_LOADER(monary_load_float32_value, FLOAT32, bson_iter_double, BSON_ITER_HOLDS_DOUBLE)
MONARY_DEFINE_NUM_LOADER(monary_load_float64_value, FLOAT64, bson_iter_double, BSON_ITER_HOLDS_DOUBLE)

int monary_load_datetime_value(const bson_iter_t* bsonit,
                               monary_column_item* citem,
                               int idx)
{
    if (BSON_ITER_HOLDS_DATE_TIME(bsonit)) {
        DATETIME value = bson_iter_date_time(bsonit);
        ((DATETIME*) citem->storage)[idx] = value;
        return 1;
    } else {
        return 0;
    }
}

int monary_load_utf8_value(const bson_iter_t* bsonit,
                           monary_column_item* citem,
                           int idx)
{
    char *dest;         // A pointer to the final location of the array in mem
    const char *src;    // Pointer to immutable buffer
    uint32_t length;    // The size of the string according to iter_utf8

    // XXX Check the sizing on these things
    if (BSON_ITER_HOLDS_UTF8(bsonit)) {
        src = bson_iter_utf8(bsonit, &length);
        if (length > citem->type_arg) {
            length = citem->type_arg;
        }
        dest = ((char*) citem->storage) + (idx * length);
        bson_strncpy(dest, src, length);
        return 1;
    } else {
        return 0;
    }
}

int monary_load_binary_value(const bson_iter_t* bsonit,
                             monary_column_item* citem,
                             int idx)
{
    bson_subtype_t subtype;
    const uint8_t *binary;
    uint32_t binary_len;

    if (BSON_ITER_HOLDS_BINARY(bsonit)) {
        // Load the binary
        bson_iter_binary(bsonit, &subtype, &binary_len, &binary);

        // Size checking
        int size = citem->type_arg;
        if(binary_len > size) {
            binary_len = size;
        }

        uint8_t *dest = ((uint8_t*) citem->storage) + (idx * size);
        memcpy(dest, binary, binary_len);
        return 1;
    } else {
        return 0;
    }
}

int monary_load_document_value(const bson_iter_t *bsonit,
                               monary_column_item *citem,
                               int idx)
{
    uint32_t document_len;      // The length of document in bytes.
    const uint8_t *document;    // Pointer to the immutable document buffer.
    uint8_t *dest;

    if (BSON_ITER_HOLDS_DOCUMENT(bsonit)) {
        bson_iter_document(bsonit, &document_len, &document);
        if (document_len > citem->type_arg) {
            document_len = citem->type_arg;
        }

        dest = ((uint8_t *) citem->storage) + (idx * document_len);
        memcpy(dest, document, document_len);
        return 1;
    }
    else {
        return 0;
    }
}

int monary_load_array_value(const bson_iter_t *bsonit,
                            monary_column_item *citem,
                            int idx)
{
    const uint8_t *array;
    uint32_t array_len;
    uint8_t *dest;

    if (BSON_ITER_HOLDS_ARRAY(bsonit)) {
        bson_iter_array(bsonit, &array_len, &array);
        if (array_len > citem->type_arg) {
            array_len = citem->type_arg;
        }
        dest = ((uint8_t *) citem->storage) + (idx * array_len);
        return 1;
    }
    else {
        return 0;
    }
}

#define MONARY_DISPATCH_TYPE(TYPENAME, TYPEFUNC)    \
case TYPENAME:                                      \
success = TYPEFUNC(bsonit, citem, offset);    \
break;

// DOUBLE
// UTF8
// DOCUMENT
// OID
// BOOL
// DATE_TIME
// INT32
// INT64
// ARRAY

int monary_load_item(const bson_iter_t* bsonit,
                     monary_column_item* citem,
                     int offset)
{
    int success = 0;

    switch(citem->type) {
        MONARY_DISPATCH_TYPE(TYPE_OBJECTID, monary_load_oid_value)
        MONARY_DISPATCH_TYPE(TYPE_DATETIME, monary_load_datetime_value)
        MONARY_DISPATCH_TYPE(TYPE_BOOL, monary_load_bool_value)

        MONARY_DISPATCH_TYPE(TYPE_INT8, monary_load_int8_value)
        MONARY_DISPATCH_TYPE(TYPE_INT16, monary_load_int16_value)
        MONARY_DISPATCH_TYPE(TYPE_INT32, monary_load_int32_value)
        MONARY_DISPATCH_TYPE(TYPE_INT64, monary_load_int64_value)

        MONARY_DISPATCH_TYPE(TYPE_UINT8, monary_load_uint8_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT16, monary_load_uint16_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT32, monary_load_uint32_value)
        MONARY_DISPATCH_TYPE(TYPE_UINT64, monary_load_uint64_value)

        MONARY_DISPATCH_TYPE(TYPE_FLOAT32, monary_load_float32_value)
        MONARY_DISPATCH_TYPE(TYPE_FLOAT64, monary_load_float64_value)

        MONARY_DISPATCH_TYPE(TYPE_UTF8, monary_load_utf8_value)
        MONARY_DISPATCH_TYPE(TYPE_BINARY, monary_load_binary_value)
        MONARY_DISPATCH_TYPE(TYPE_DOCUMENT, monary_load_document_value)
        MONARY_DISPATCH_TYPE(TYPE_ARRAY, monary_load_array_value)
    }

    return success;
}

/**
 * Copies over raw BSON data into Monary column storage. This function
 * determines the types of the data, dispatches to an appropriate handler and
 * copies over the data. It keeps a count of any unsuccessful loads and sets
 * NumPy-compatible masks on the data as appropriate.
 *
 * @param coldata A pointer to monary_column_data which contains the final
 * storage location for the BSON data.
 * @param row The row number to store the data in. Cannot exceed
 * coldata->num_columns.
 * @param bson_data A pointer to an immutable BSON data buffer.
 *
 * @return The number of unsuccessful loads.
 */
int monary_bson_to_arrays(monary_column_data* coldata,
                          unsigned int row,
                          const bson_t* bson_data)
{
    bson_type_t found_type;
    int num_masked = 0;
    int i;
    bson_iter_t bsonit;

    for(i = 0; i < coldata->num_columns; i++) {
        int offset = row;
        int success = 0;
        monary_column_item* citem = coldata->columns + i;
        
        // TODO: Ask Jason: is this okay? Aren't we skipping over other
        // potential fields? etc.
        // I need to look at the old C driver and see how that worked out
        if (bson_iter_init_find(&bsonit, bson_data, citem->field)) {
            // Determine the BSON type of the observed item on the iterator
            // TODO I think this is broken now
            found_type = bson_iter_type(&bsonit);

            // Dispatch to appropriate column handler
            if (found_type) {
                // TODO The function signature has changed
                success = monary_load_item(&bsonit, citem, offset);
            }
        }

        // Record success result in mask, if applicable
        if (citem->mask != NULL) {
            citem->mask[offset] = !success;
        }

        // tally number of masked (unsuccessful) loads
        if(!success) { ++num_masked; }
    }
    
    return num_masked;
}

/**
 * Performs a count query on a MongoDB collection.
 *
 * @param collection The MongoDB collection to query against.
 * @param query A valid JSON-compatible UTF-8 string query.
 *
 * @return If unsuccessful, returns -1; otherwise, returns the number of
 * documents counted.
 */
int64_t monary_query_count(mongoc_collection_t *collection,
                           const uint8_t *query)
{
    bson_error_t error;     // A location for BSON errors
    bson_t query_bson;      // The query converted to BSON format
    int64_t total_count;    // The number of documents counted

    // build BSON query data
    bson_init_static(&query_bson,
                     query,
                     bson_strnlen(query, MONARY_MAX_QUERY_LENGTH));
    
    // Make the count query
    total_count = mongoc_collection_count(collection,
                                          MONGOC_QUERY_NONE,
                                          &query_bson,
                                          0,
                                          0,
                                          NULL,
                                          &error);
    bson_destroy(&query_bson);
    if (total_count < 0) {
        DEBUG("error: %d.%d %s", error.domain, error.code, error.message);
    }

    return total_count;
}

/**
 * Given pre-allocated array data that specifies the fields to find, this
 * builds a BSON document that can be passed into a MongoDB query.
 * XXX: Perhaps this can be moved into query(), as it is only called there.
 *
 * @param coldata A pointer to a monary_column_data, which should have already
 * been allocated and built properly. The names of the fields of its column
 * items become the names of the fields to query for.
 * @param fields_bson A pointer to a bson_t that should already be initialized.
 * After this BSON is written to, it may be used in a query and then destroyed
 * afterwards.
 */
void monary_get_bson_fields_list(monary_column_data* coldata,
                                 bson_t* fields_bson)
{
    int i;
    monary_column_item *col;

    // We want to select exactly each field specified in coldata, of which
    // there are exactly coldata.num_columns
    for (i = 0; i < coldata->num_columns; i++) {
        col = coldata->columns + i;
        // TODO: Is it strlen or strlen+1?
        bson_append_int32(fields_bson, col->field, strlen(col->field), 1);
    }
}

/**
 * Performs a find query on a MongoDB collection, selecting certain fields from
 * the results and storing them in Monary columns.
 *
 * @param collection The MongoDB collection to query against.
 * @param offset The number of documents to skip, or zero.
 * @param limit The maximum number of documents to return, or zero.
 * @param query A valid JSON-compatible UTF-8 string query. This function does
 * no validation, so you should ensure that the query is well-formatted.
 * @param coldata The column data to store the results in.
 * @param select_fields If truthy, select exactly the fields from the database
 * that match the fields in coldata. If false, the query will find and return
 * all fields from matching documents.
 *
 * @return If successful, a Monary cursor that should be freed with
 * monary_close_query() when no longer in use. If unsuccessful, or if an
 * invalid query was passed in, NULL is returned.
 */
monary_cursor* monary_init_query(mongoc_collection_t *collection,
                                 uint32_t offset,
                                 uint32_t limit,
                                 const uint8_t *query,
                                 monary_column_data *coldata,
                                 int select_fields)
{
    // XXX Code Review
    bson_t query_bson;          // BSON representing the query to perform
    bson_t *fields_bson;        // BSON holding the fields to select
    bson_error_t error;         // Location for libbson-related errors
    mongoc_cursor_t *mcursor;   // A MongoDB cursor

    // Sanity checks
    if (!collection || !query || !coldata) {
        return NULL;
    }

    // build BSON query data
    bson_init_static(&query_bson,
                     query,
                     bson_strnlen(query, MONARY_MAX_QUERY_LENGTH));
    fields_bson = NULL;


    // build BSON fields list (if necessary)
    if(select_fields) {
        // Initialize BSON on the heap, as it will grow
        fields_bson = bson_new();
        if (!fields_bson) {
            DEBUG("An error occurred while allocating memory for BSON data.");
            return NULL;
        }
        monary_get_bson_fields_list(coldata, &query_bson);
    }

    // create query cursor
    mcursor = mongoc_collection_find(collection,
                                     MONGOC_QUERY_NONE,
                                     offset,
                                     limit,
                                     0,
                                     &query_bson,
                                     fields_bson,
                                     NULL);

    // destroy BSON fields
    bson_destroy(&query_bson);
    if(fields_bson) { bson_destroy(&fields_bson); }

    // finally, create a new Monary cursor
    monary_cursor* cursor = (monary_cursor*) malloc(sizeof(monary_cursor));
    if (!cursor) {
        DEBUG("malloc(3) failed to allocate a Monary cursor");
        mongoc_cursor_destroy(mcursor);
        return NULL;
    }
    else {
        cursor->mcursor = mcursor;
        cursor->coldata = coldata;
        return cursor;
    }
}

/**
 * Grabs the results obtained from the MongoDB cursor and loads them into
 * in-memory arrays.
 *
 * @param cursor A pointer to a Monary cursor, which contains both a MongoDB
 * cursor and Monary column data that stores the retrieved information.
 *
 * @return The number of rows loaded into memory.
 */
int monary_load_query(monary_cursor* cursor)
{
    bson_error_t error;             // A location for errors
    const bson_t *bson;             // Pointer to an immutable BSON buffer
    int num_masked;
    int row;
    monary_column_data *coldata;
    mongoc_cursor_t *mcursor;

    mcursor = cursor->mcursor;  // The underlying MongoDB cursor
    coldata = cursor->coldata;  // A pointer to the NumPy array data
    row = 0;                    // Iterator var over the lengths of the arrays
    num_masked = 0;             // The number of failed loads
    
    // read result values
    while(row < coldata->num_rows
            && !mongoc_cursor_error(mcursor, &error)
            && mongoc_cursor_more(mcursor)) {

#ifndef NDEBUG
        if(row % 500000 == 0) {
            DEBUG("...%i rows loaded", row);
        }
#endif

        if (mongoc_cursor_next(mcursor, &bson)) {
            num_masked += monary_bson_to_arrays(coldata, row, bson);
        }
        ++row;
    }

    if (mongoc_cursor_error(mcursor, &error)) {
        DEBUG("error: %d.%d %s", error.domain, error.code, error.message);
    }

    int total_values = row * coldata->num_columns;
    DEBUG("%i rows loaded; %i / %i values were masked", row, num_masked, total_values);

    bson_destroy(bson);
    return row;
}

/**
 * Destroys the underlying MongoDB cursor associated with the given cursor.
 *
 * Note that the column data is not freed in this function as that data is
 * exposed as NumPy arrays in Python.
 *
 * @param cursor A pointer to the Monary cursor to close. If cursor is NULL,
 * no operation is performed.
 */
void monary_close_query(monary_cursor* cursor)
{
    if (cursor) {
        mongoc_cursor_destroy(cursor->mcursor);
        free(cursor);
    }
}
